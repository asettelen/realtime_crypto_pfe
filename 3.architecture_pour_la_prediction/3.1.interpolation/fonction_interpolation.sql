CREATE OR REPLACE FUNCTION timeseries.tumble_interval(
 val TIMESTAMP, tumble_seconds INT64)
AS (
 timestamp_seconds(div(UNIX_SECONDS(val), tumble_seconds) *  tumble_seconds)
);

CREATE OR REPLACE FUNCTION
timeseries.gen_ts_candidates(keys ARRAY<STRING>, tumble_seconds INT64, min_ts TIMESTAMP, max_ts Timestamp)
AS ((
 SELECT ARRAY_AGG(x)
 FROM (
   SELECT series_key, tumble_val
   FROM UNNEST(
     GENERATE_TIMESTAMP_ARRAY(
       timeseries.tumble_interval(min_ts, tumble_seconds),
       timeseries.tumble_interval(max_ts, tumble_seconds),
       INTERVAL tumble_seconds SECOND
     )
   ) AS tumble_val
   CROSS JOIN UNNEST(keys) AS series_key
 ) x
));

CREATE OR REPLACE function
timeseries.linear_interpolate(pos INT64,
        prev STRUCT<x INT64,y FLOAT64>,
        next STRUCT<x INT64,y FLOAT64>)
AS (
CASE
  WHEN prev IS NULL OR next IS NULL THEN null
  ELSE
  --y = m * x + b
  (next.y - prev.y)/(next.x - prev.x) * (pos - prev.x) + prev.y
END
);

CREATE OR REPLACE FUNCTION
timeseries.interpolate_ts_val_or_fill(value FLOAT64,
        pos TIMESTAMP,
        prev STRUCT<x TIMESTAMP, y FLOAT64>,
        next STRUCT<x TIMESTAMP, y FLOAT64>,
        def FLOAT64)
AS (
 CASE
   WHEN value IS NOT NULL THEN value
   WHEN prev IS NULL OR next IS NULL THEN def
   ELSE
     timeseries.linear_interpolate(
       unix_seconds(pos),
       STRUCT(unix_seconds(prev.x) AS x, prev.y AS y),
       STRUCT(unix_seconds(next.x) AS x, next.y AS y)
     )
 END
);





