from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

query = '''
CREATE OR REPLACE MODEL timeseries.arima_model_interpo_hour_global
OPTIONS
  (model_type = 'ARIMA_PLUS',
   time_series_timestamp_col = 'tumble',
   time_series_data_col = 'intrp',
   time_series_id_col = 'symbol',
   auto_arima = TRUE,
   data_frequency = 'AUTO_FREQUENCY',
   decompose_time_series = TRUE, 
   auto_arima_max_order = 5
  ) AS
SELECT
symbol, tumble, avg(intrp) as intrp
FROM
  `temp_crypto_batch.batch_hour_interpo`
WHERE 
            tumble <= (SELECT TIMESTAMP_SUB(max(tumble), INTERVAL 5 DAY) FROM 
            `temp_crypto_batch.batch_hour_interpo`)
GROUP BY symbol, tumble; 
'''

# Construct a BigQuery client object.
client = bigquery.Client() 

def hello_pubsub(event=None, context=None):
     
    job_config = bigquery.QueryJobConfig()
    client.query(query, job_config=job_config)
 
    return "Function ran successfully"