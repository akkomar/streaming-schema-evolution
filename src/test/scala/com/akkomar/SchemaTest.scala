package com.akkomar

import java.io.File
import java.time.{ZoneId, ZonedDateTime}

import com.akkomar.Ping.BaseTs
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{count, lit, sum, window}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class SchemaTest extends FlatSpec with BeforeAndAfterEach with Matchers {

  val streamingOutputPath = "/tmp/parquet"
  val streamingCheckpointPath = "/tmp/checkpoint"
  val spark = SparkSession.builder()
    .appName("Schema evolution test")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  implicit val sqlContext = spark.sqlContext

  def baseQuery(pings: MemoryStream[Ping], additionalMetrics: Seq[Column] = Seq(), additionalDimensions: Seq[Column] = Seq()): StreamingQuery = {
    import pings.sqlContext.implicits._
    pings.toDS()
      .withColumn("timestamp", ($"ts" / 1000).cast(TimestampType))
      .withWatermark("timestamp", "1 minute")
      .groupBy(window($"timestamp", "5 minute") +: $"osName" +: additionalDimensions: _*)
      .agg(count(lit(1)).as("count"), additionalMetrics.map(m => sum(m).as(m.toString())): _*)
      .writeStream
      .option("path", streamingOutputPath)
      .option("checkpointLocation", streamingCheckpointPath)
      .outputMode(OutputMode.Append)
      .start()
  }

  override protected def beforeEach(): Unit = {
    cleanupTestDirectories()
  }

  override protected def afterEach(): Unit = {
    cleanupTestDirectories()
  }

  private def cleanupTestDirectories() = {
    FileUtils.deleteDirectory(new File(streamingOutputPath))
    FileUtils.deleteDirectory(new File(streamingCheckpointPath))
  }

  "Streaming query with aggregations" can "resume with the same query" in {
    println("No schema change")
    val pings = MemoryStream[Ping]

    val query = baseQuery(pings)

    pings.addData(Seq(Ping("a"), Ping("b")))
    query.processAllAvailable()
    pings.addData(Seq(Ping("c", ts = BaseTs.add(minutes = 7))))
    query.processAllAvailable()
    pings.addData(Ping.Dummy)
    query.processAllAvailable()

    query.stop()

    println("Base query output:")
    val out = spark.read.parquet(s"$streamingOutputPath")
    out.sort($"window").show(false)
    out.select("window", "osName", "count").as[(Window, String, Long)].collect() should contain theSameElementsAs Seq(
      (Window("2018-01-01 13:00:00, 2018-01-01 13:05:00"), "Win", 2)
    )


    val query2 = baseQuery(pings)

    pings.addData(Seq(Ping("c", ts = BaseTs.add(minutes = 11))))
    query2.processAllAvailable()
    pings.addData(Ping.Dummy)
    query2.processAllAvailable()
    query2.stop()

    println("Restarted query output:")
    val out2 = spark.read.parquet(s"$streamingOutputPath").sort($"window")
    out2.show(false)
    out2.select("window", "osName", "count").as[(Window, String, Long)].collect() should contain theSameElementsAs Seq(
      (Window("2018-01-01 13:00:00, 2018-01-01 13:05:00"), "Win", 2),
      (Window("2018-01-01 13:05:00, 2018-01-01 13:10:00"), "Win", 1)
    )
  }

  it should "resume if new metric is added (WARNING - seems to be non-deterministic)" in {
    println("Adding metric")
    val pings = MemoryStream[Ping]

    val query = baseQuery(pings)

    pings.addData(Seq(Ping("a"), Ping("b")))
    query.processAllAvailable()
    pings.addData(Seq(Ping("c", ts = BaseTs.add(minutes = 7))))
    query.processAllAvailable()
    pings.addData(Ping.Dummy)
    query.processAllAvailable()

    query.stop()

    println("Base query output:")
    spark.read.parquet(s"$streamingOutputPath").sort($"window").show(false)
    val query2 = baseQuery(pings, additionalMetrics = Seq($"subsessionCount"))

    pings.addData(Ping("c", ts = BaseTs.add(minutes = 11)), Ping("d", ts = BaseTs.add(minutes = 14)))
    query2.processAllAvailable()
    pings.addData(Ping("a", ts = BaseTs.add(minutes = 20)))
    query2.processAllAvailable()
    pings.addData(Ping.Dummy)
    query2.processAllAvailable()
    query2.stop()

    println("Restarted query with new metric output:")
    val out = spark.read.parquet(s"$streamingOutputPath").sort($"window")
    out.show(false)
    out.select("window", "osName", "count", "subsessionCount").as[(Window, String, Long, Long)].collect() should contain theSameElementsAs Seq(
      (Window("2018-01-01 13:00:00, 2018-01-01 13:05:00"), "Win", 2, -1), // default value (-1) in place of newly added metric
      (Window("2018-01-01 13:05:00, 2018-01-01 13:10:00"), "Win", 1, 0),
      (Window("2018-01-01 13:10:00, 2018-01-01 13:15:00"), "Win", 2, 2)
    )
  }

  it should "resume if metrics are removed" in {
    println("Removing metric")
    val pings = MemoryStream[Ping]

    val query = baseQuery(pings, additionalMetrics = Seq($"subsessionCount"))

    pings.addData(Seq(Ping("a"), Ping("b")))
    query.processAllAvailable()
    pings.addData(Seq(Ping("c", ts = BaseTs.add(minutes = 7))))
    query.processAllAvailable()
    pings.addData(Ping.Dummy)
    query.processAllAvailable()

    query.stop()

    println("Base query output:")
    val out = spark.read.parquet(s"$streamingOutputPath").sort($"window")
    out.show(false)
    out.select("window", "osName", "count","subsessionCount").as[(Window, String, Long,Long)].collect() should contain theSameElementsAs Seq(
      (Window("2018-01-01 13:00:00, 2018-01-01 13:05:00"), "Win", 2,2)
    )

    val query2 = baseQuery(pings)

    pings.addData(Ping("c", ts = BaseTs.add(minutes = 11)), Ping("d", ts = BaseTs.add(minutes = 14)))
    query2.processAllAvailable()
    pings.addData(Ping("a", ts = BaseTs.add(minutes = 20)), Ping("a", ts = BaseTs.add(minutes = 21)))
    query2.processAllAvailable()
    pings.addData(Ping.Dummy)
    query2.processAllAvailable()
    query2.stop()

    println("Restarted query with new metric output:")
    val out2 = spark.read.parquet(s"$streamingOutputPath").sort($"window")
    out2.show(false)
    out2.select("window", "osName", "count").as[(Window, String, Long)].collect() should contain theSameElementsAs Seq(
      (Window("2018-01-01 13:00:00, 2018-01-01 13:05:00"), "Win", 2),
      (Window("2018-01-01 13:05:00, 2018-01-01 13:10:00"), "Win", 1),
      (Window("2018-01-01 13:10:00, 2018-01-01 13:15:00"), "Win", 2)
    )
  }

  it can "resume if new dimension is added (WARNING: undefined dimension value for checkpointed state)" in {
    println("Adding dimension")
    val pings = MemoryStream[Ping]

    val query = baseQuery(pings)

    pings.addData(Seq(Ping("a"), Ping("b")))
    query.processAllAvailable()
    pings.addData(Seq(Ping("c", ts = BaseTs.add(minutes = 7))))
    query.processAllAvailable()
    pings.addData(Ping.Dummy)
    query.processAllAvailable()

    query.stop()

    println("Base query output:")
    spark.read.parquet(s"$streamingOutputPath").sort($"window").show(false)
    val query2 = baseQuery(pings, additionalDimensions = Seq($"osVersion"))

    pings.addData(Ping("c", ts = BaseTs.add(minutes = 11)), Ping("d", osVersion="10", ts = BaseTs.add(minutes = 14)))
    query2.processAllAvailable()
    pings.addData(Ping("a", ts = BaseTs.add(minutes = 20)))
    query2.processAllAvailable()
    pings.addData(Ping.Dummy)
    query2.processAllAvailable()
    query2.stop()

    println("Restarted query with new metric output:")
    val out = spark.read.parquet(s"$streamingOutputPath").sort($"window")
    out.show(false)
    out.select("window", "osName", "osVersion", "count").as[(Window, String, String, Long)].collect() should contain theSameElementsAs Seq(
      (Window("2018-01-01 13:00:00, 2018-01-01 13:05:00"), "Win", null, 2),
      (Window("2018-01-01 13:05:00, 2018-01-01 13:10:00"), "Win", Integer.parseInt("20", 16).toChar, 0),
      (Window("2018-01-01 13:10:00, 2018-01-01 13:15:00"), "Win", "10", 1),
      (Window("2018-01-01 13:10:00, 2018-01-01 13:15:00"), "Win", "7", 1)
    )
    val res = out.select("window", "osName", "osVersion", "count").as[(Window, String, String, Long)].collect()
    println(res(1)._3)
  }

  it should "resume if dimension is removed (WARNING: undefined metric value for checkpointed state)" in {
    println("Removing dimension")
    val pings = MemoryStream[Ping]

    val query = baseQuery(pings, additionalDimensions = Seq($"osVersion"))

    pings.addData(Seq(Ping("a"), Ping("b")))
    query.processAllAvailable()
    pings.addData(Seq(Ping("c", ts = BaseTs.add(minutes = 7))))
    query.processAllAvailable()
    pings.addData(Ping.Dummy)
    query.processAllAvailable()

    query.stop()

    println("Base query output:")
    spark.read.parquet(s"$streamingOutputPath").sort($"window").show(false)
    val query2 = baseQuery(pings)

    pings.addData(Ping("c", ts = BaseTs.add(minutes = 11)), Ping("d", ts = BaseTs.add(minutes = 14)))
    query2.processAllAvailable()
    pings.addData(Ping("c", ts = BaseTs.add(minutes = 21)))
    query2.processAllAvailable()
    pings.addData(Ping.Dummy)
    query2.processAllAvailable()
    query2.stop()

    println("Restarted query with new metric output:")
    val out = spark.read.parquet(s"$streamingOutputPath").sort($"window")
    out.show(false)
    out.select("window", "osName", "count").as[(Window, String, Long)].collect() should contain theSameElementsAs Seq(
      (Window("2018-01-01 13:00:00, 2018-01-01 13:05:00"), "Win", 2),
      (Window("2018-01-01 13:05:00, 2018-01-01 13:10:00"), "Win", 309237645313l),
      (Window("2018-01-01 13:10:00, 2018-01-01 13:15:00"), "Win", 2)
    )
  }
}

object Ping {
  val baseTs = ZonedDateTime.of(2018, 1, 1, 12, 0, 0, 0, ZoneId.of("UTC"))
  val Dummy = Ping("x", ts = BaseTs.add(minutes = -11)) //ping from the past, for advancing watermark

  object BaseTs {
    def add(hours: Int = 0, minutes: Int = 0): Long = {
      baseTs.plusHours(hours).plusMinutes(minutes).toInstant.toEpochMilli
    }
  }
}

case class Ping(clientId: String, osName: String = "Win", osVersion: String = "7", subsessionCount: Int = 1, ts: Long = Ping.baseTs.toInstant.toEpochMilli)


case class Window(start: java.sql.Timestamp, end: java.sql.Timestamp)

object Window {
  def apply(windowString: String): Window = {
    val range = windowString.split(", ")
    Window(java.sql.Timestamp.valueOf(range(0)), java.sql.Timestamp.valueOf(range(1)))
  }
}