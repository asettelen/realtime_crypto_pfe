import logging
import google.auth
from googleapiclient.discovery import build
from datetime import datetime
import datetime as dt
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

#start_timestamp
def round_up_to_minute(timestamp):
    return timestamp + dt.timedelta(
        seconds=60-timestamp.second,
        microseconds=-timestamp.microsecond)

#end_timestamp
def round_up_to_2_minutes(timestamp):
    return timestamp + dt.timedelta(
        seconds=120-timestamp.second,
        microseconds=-timestamp.microsecond)

def hello_pubsub(event=None, context=None):
    
    query = '''
                select max(timestamp)
                from 
                test_crypto_batch.test_crypto_streaming_11
            '''

    # Construct a BigQuery client object.
    client = bigquery.Client() 

    job_config = bigquery.QueryJobConfig()
    query_df = client.query(query, job_config=job_config).result().to_dataframe()
      
    now = datetime.now()
    project_topic_path = 'projects/' + google.auth.default()[1] + '/topics/bigquery-to-pubsub-test0'
    temp_resource_name = google.auth.default()[1].replace("-", "_") + '_bigquery_to_pubsub_temp'
    job_name = 'auto_streaming_with_ai_platform' + now.strftime("%d%m%Y%H%M")
    
    training_inputs = {
    'region': 'us-central1',
    'masterConfig': {
      "imageUri": "gcr.io/adept-odyssey-314211/bigquery-to-pubsub@sha256:bb6af4c9a0fd07232bd49c01a82380e0bd9744f6d51b6f0028e13a090169d144"
     }, 
    'args': ['--bigquery-table', 'public-data-finance.crypto_band.oracle_requests', 
             '--timestamp-field', 'block_timestamp_truncated',
             '--start-timestamp', round_up_to_minute(query_df.values[0][0]).strftime('%Y-%m-%dT%H:%M:%S'), 
             '--end-timestamp', round_up_to_2_minutes(query_df.values[0][0]).strftime('%Y-%m-%dT%H:%M:%S'), 
             '--batch-size-in-seconds', '3600', 
             '--replay-rate', '1', 
             '--pubsub-topic', project_topic_path, 
             '--temp-bigquery-dataset', temp_resource_name,
             '--temp-bucket', temp_resource_name
            ]
    }

    job_spec = {'jobId': job_name, 'trainingInput': training_inputs}
    
    project_id = 'projects/{}'.format(google.auth.default()[1])
    
    credentials, _ = google.auth.default()
    
    cloudml = build('ml', 'v1', credentials=credentials)
    
    request = cloudml.projects().jobs().create(body=job_spec,
              parent=project_id)
    
    response = request.execute()
    logging.info(f'response status: {response}')
    
    return "Function ran successfully" 