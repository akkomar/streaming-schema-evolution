# Spark Structured Streaming schema evolution exploration

This is exploration of aggregation queries' behaviour on schema changes.
Focus is on testing if query updates can be done in-place and reuse previous version's checkpoints.

Four cases are tested:
* Adding new metric
* Removing metric
* Adding new dimension
* Removing dimension

Each test consists of three steps:
1. Running 'base' version of query (may differ from case to case) and shutting it down
2. Restarting new, changed query using checkpoint data saved in previous step
3. Examining results

## Running tests
```bash
sbt test
```

## Results
Apart from removing metric, all the cases pose some problems - either undefined values for checkpointed state,
or non-deterministic behaviour with regard to added columns.

It seems to be safer to restart updated queries with clean state, ideally starting from earliest offsets of oldest open window 
(or, if it's not present, end offsets of youngest closed window).

__TODO:__ confirm if these offsets can be easily retrieved from checkpoints.

### Adding metric
```
Base query output:
+------------------------------------------+------+-----+
|window                                    |osName|count|
+------------------------------------------+------+-----+
|[2018-01-01 13:00:00, 2018-01-01 13:05:00]|Win   |2    |
+------------------------------------------+------+-----+

Restarted query with new metric output:
+------------------------------------------+------+-----+---------------+
|window                                    |osName|count|subsessionCount|
+------------------------------------------+------+-----+---------------+
|[2018-01-01 13:00:00, 2018-01-01 13:05:00]|Win   |2    |null           |
|[2018-01-01 13:05:00, 2018-01-01 13:10:00]|Win   |1    |0              |
|[2018-01-01 13:10:00, 2018-01-01 13:15:00]|Win   |2    |2              |
+------------------------------------------+------+-----+---------------+
```
* First row's `null` - data written before change
* Second's row `0` - data written after the change, but state checkpointed before
* __Result is non-deterministic__, it sometimes outputs:
    ```
    +------------------------------------------+------+-----+
    |window                                    |osName|count|
    +------------------------------------------+------+-----+
    |[2018-01-01 13:00:00, 2018-01-01 13:05:00]|Win   |2    |
    |[2018-01-01 13:05:00, 2018-01-01 13:10:00]|Win   |1    |
    |[2018-01-01 13:10:00, 2018-01-01 13:15:00]|Win   |2    |
    +------------------------------------------+------+-----+
    ```

### Removing metric
```
Base query output:
+------------------------------------------+------+-----+---------------+
|window                                    |osName|count|subsessionCount|
+------------------------------------------+------+-----+---------------+
|[2018-01-01 13:00:00, 2018-01-01 13:05:00]|Win   |2    |2              |
+------------------------------------------+------+-----+---------------+

Restarted query with new metric output:
+------------------------------------------+------+-----+
|window                                    |osName|count|
+------------------------------------------+------+-----+
|[2018-01-01 13:00:00, 2018-01-01 13:05:00]|Win   |2    |
|[2018-01-01 13:05:00, 2018-01-01 13:10:00]|Win   |1    |
|[2018-01-01 13:10:00, 2018-01-01 13:15:00]|Win   |2    |
+------------------------------------------+------+-----+
```
Looks good.

### Adding dimension
```
Base query output:
+------------------------------------------+------+-----+
|window                                    |osName|count|
+------------------------------------------+------+-----+
|[2018-01-01 13:00:00, 2018-01-01 13:05:00]|Win   |2    |
+------------------------------------------+------+-----+

Restarted query with new metric output:
+------------------------------------------+------+---------+-----+
|window                                    |osName|osVersion|count|
+------------------------------------------+------+---------+-----+
|[2018-01-01 13:00:00, 2018-01-01 13:05:00]|Win   |null     |2    |
|[2018-01-01 13:05:00, 2018-01-01 13:10:00]|Win   |         |0    |
|[2018-01-01 13:10:00, 2018-01-01 13:15:00]|Win   |10       |1    |
|[2018-01-01 13:10:00, 2018-01-01 13:15:00]|Win   |7        |1    |
+------------------------------------------+------+---------+-----+
```
__Result is undefined for checkpointed state__ (2nd row's `osVersion` is space).
Also sometimes output does not contain `osVersion` column:
```
+------------------------------------------+------+-----+
|window                                    |osName|count|
+------------------------------------------+------+-----+
|[2018-01-01 13:00:00, 2018-01-01 13:05:00]|Win   |2    |
|[2018-01-01 13:05:00, 2018-01-01 13:10:00]|Win   |0    |
|[2018-01-01 13:10:00, 2018-01-01 13:15:00]|Win   |1    |
|[2018-01-01 13:10:00, 2018-01-01 13:15:00]|Win   |1    |
+------------------------------------------+------+-----+
```

### Removing dimension
```
Base query output:
+------------------------------------------+------+---------+-----+
|window                                    |osName|osVersion|count|
+------------------------------------------+------+---------+-----+
|[2018-01-01 13:00:00, 2018-01-01 13:05:00]|Win   |7        |2    |
+------------------------------------------+------+---------+-----+

Restarted query with new metric output:
+------------------------------------------+------+---------+------------+
|window                                    |osName|osVersion|count       |
+------------------------------------------+------+---------+------------+
|[2018-01-01 13:00:00, 2018-01-01 13:05:00]|Win   |7        |2           |
|[2018-01-01 13:05:00, 2018-01-01 13:10:00]|Win   |null     |309237645313|
|[2018-01-01 13:10:00, 2018-01-01 13:15:00]|Win   |null     |2           |
+------------------------------------------+------+---------+------------+
```
__Metric value for checkpointed state is undefined.__











