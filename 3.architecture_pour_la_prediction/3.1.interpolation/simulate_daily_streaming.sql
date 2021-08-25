CREATE OR REPLACE TABLE
  temp_crypto_batch.streaming_global_simulated AS
SELECT
  *
FROM (
  WITH
    parsed_aggregator_oracle_requests AS (
    SELECT
      ARRAY(
      SELECT
        JSON_EXTRACT_SCALAR(symbol_as_json,
          '$')
      FROM
        UNNEST(JSON_EXTRACT_ARRAY(decoded_result.calldata,
            "$.symbols")) AS symbol_as_json ) AS symbols,
      CAST(JSON_EXTRACT_SCALAR(decoded_result.calldata,
          "$.multiplier") AS FLOAT64) AS multiplier,
      ARRAY(
      SELECT
        CAST(JSON_EXTRACT_SCALAR(rate_as_json,
            '$') AS FLOAT64)
      FROM
        UNNEST(JSON_EXTRACT_ARRAY(decoded_result.result,
            "$.rates")) AS rate_as_json ) AS rates,
      block_timestamp,
      block_timestamp_truncated,
      oracle_request_id,
    FROM
      `public-data-finance.crypto_band.oracle_requests`
    WHERE
      request.oracle_script_id = 3
      AND decoded_result.calldata IS NOT NULL
      AND decoded_result.result IS NOT NULL ),
    zipped_rates AS (
    SELECT
      block_timestamp,
      block_timestamp_truncated,
      oracle_request_id,
      STRUCT(symbol,
        rates[
      OFFSET
        (off)] AS rate) AS zipped,
      multiplier,
    FROM
      parsed_aggregator_oracle_requests,
      UNNEST(symbols) AS symbol
    WITH
    OFFSET
      off
    WHERE
      ARRAY_LENGTH(symbols) = ARRAY_LENGTH(rates) ),
    adjusted_rates AS (
    SELECT
      block_timestamp,
      block_timestamp_truncated,
      oracle_request_id,
      STRUCT(zipped.symbol,
        IEEE_DIVIDE(zipped.rate, multiplier) AS rate) AS zipped,
    FROM
      zipped_rates )
  SELECT
    block_timestamp_truncated AS timestamp,
    CAST(oracle_request_id AS STRING) AS request,
    zipped.symbol AS symbol,
    zipped.rate AS rate,
    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', block_timestamp_truncated) AS date_str,
  FROM
    adjusted_rates
  WHERE
    block_timestamp_truncated >= '2021-05-01 00:00:00.000000 UTC'
    AND block_timestamp_truncated < (
    SELECT
      TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)))
  ORDER BY
    block_timestamp_truncated DESC)