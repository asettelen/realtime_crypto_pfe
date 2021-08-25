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
def round_up_to_4_minutes(timestamp):
    return timestamp + dt.timedelta(
        seconds=300-timestamp.second,
        microseconds=-timestamp.microsecond)

def hello_pubsub(event=None, context=None):
    
    project = google.auth.default()[1]
    project
    
    query = '''
                select max(timestamp)
                from 
                `{project}.temp_crypto_batch.streaming_timestamp`
            '''.format(project=project)

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
      "imageUri": "gcr.io/verdant-cargo-321713/bigquery-to-pubsub@sha256:69565ff4626ba9859b3b6d0c17b11702f9f5b5fe4c03eea5b1ca384683bbe4b3" \
        .format(project=project)
     }, 
    'args': ['--bigquery-table', 'public-data-finance.crypto_band.oracle_requests', 
             '--timestamp-field', 'block_timestamp_truncated',
             '--start-timestamp', round_up_to_minute(query_df.values[0][0]).strftime('%Y-%m-%dT%H:%M:%S'), 
             '--end-timestamp', round_up_to_4_minutes(query_df.values[0][0]).strftime('%Y-%m-%dT%H:%M:%S'), 
             '--batch-size-in-seconds', '3600', 
             '--replay-rate', '1', 
             '--pubsub-topic', project_topic_path, 
             '--temp-bigquery-dataset', temp_resource_name,
             '--temp-bucket', temp_resource_name
            ]
    }
    
    ########### LAUNCHING SUBSCRIPTION ############ 
    
    credentials, _ = google.auth.default()
    service = build('dataflow', 'v1b3', credentials=credentials)
    
    template_path="gs://{project}_bigquery_to_pubsub_temp/templates/CUSTOM_TEMPLATE_DATAFLOW_STREAMING" \
        .format(project=project.replace("-", "_"))
    
    template_body = {
        "jobName": ("launch-streaming-after-batch-dataflow"),
    }
    
    request = service.projects().templates().launch(
                    projectId=google.auth.default()[1],
                    gcsPath=template_path,
                    body=template_body
    )
    
    response = request.execute()
    
    logging.info(f'response status: {response}')
    
    ############ END SUBSCRIPTION ##########

    job_spec = {'jobId': job_name, 'trainingInput': training_inputs}
    
    project_id = 'projects/{}'.format(google.auth.default()[1])
    
    credentials, _ = google.auth.default()
    
    cloudml = build('ml', 'v1', credentials=credentials)
    
    request = cloudml.projects().jobs().create(body=job_spec,
              parent=project_id)
    
    response = request.execute()
    logging.info(f'response status: {response}')
    
    return "Function ran successfully" 