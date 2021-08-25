CREATE OR REPLACE TABLE
  temp_crypto_batch.batch_hour_interpo AS
SELECT
  *
FROM (
  WITH
    requests AS (
    SELECT
      *,
    IF
      (count IS NULL,
        NULL,
        STRUCT(tumble AS x,
          mean AS y) ) AS coord
    FROM (
      SELECT
        symbol,
        timeseries.tumble_interval(timestamp,
          3600) tumble,
        AVG(rate) AS mean,
        COUNT(*) AS count
      FROM
        `sharp-memento-321112.temp_crypto_batch.batch_timestamp`
      GROUP BY
        symbol,
        tumble
      UNION ALL
      SELECT
        symbol,
        timeseries.tumble_interval(timestamp,
          3600) tumble,
        AVG(rate) AS mean,
        COUNT(*) AS count
      FROM
        `sharp-memento-321112.temp_crypto_batch.streaming_global_simulated`
      GROUP BY
        symbol,
        tumble
      UNION ALL
      SELECT
        symbol,
        timeseries.tumble_interval(timestamp,
          3600) tumble,
        AVG(rate) AS mean,
        COUNT(*) AS count
      FROM
        `sharp-memento-321112.temp_crypto_batch.validation_timestamp`
      GROUP BY
        symbol,
        tumble ) ),
    args AS (
    SELECT
      ARRAY_AGG(DISTINCT symbol) AS key,
      MIN(tumble) AS min_ts,
      MAX(tumble) AS max_ts
    FROM
      requests )
  SELECT
    series_key AS symbol,
    tumble_val AS tumble,
    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', tumble_val) AS date_str,
    timeseries.interpolate_ts_val_or_fill( mean,
      tumble_val,
      LAST_VALUE(coord IGNORE NULLS) OVER (PARTITION BY series_key ORDER BY UNIX_SECONDS(tumble_val) ASC RANGE BETWEEN UNBOUNDED PRECEDING
        AND 1 PRECEDING),
      FIRST_VALUE(coord IGNORE NULLS) OVER (PARTITION BY series_key ORDER BY UNIX_SECONDS(tumble_val) ASC RANGE BETWEEN 1 FOLLOWING
        AND UNBOUNDED FOLLOWING),
      0 ) AS intrp,
    mean AS unfilled,
  IF
    (count IS NULL,
      0,
      count) AS count
  FROM
    UNNEST( (
      SELECT
        timeseries.gen_ts_candidates(key,
          3600,
          min_ts,
          max_ts)
      FROM
        args) ) a
  LEFT OUTER JOIN
    requests b
  ON
    a.series_key = b.symbol
    AND a.tumble_val = b.tumble
  ORDER BY
    symbol,
    tumble_val DESC)