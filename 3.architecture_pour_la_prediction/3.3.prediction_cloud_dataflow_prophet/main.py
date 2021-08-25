import logging
import google.auth
from googleapiclient.discovery import build

def hello_pubsub(event=None, context=None):
    
    credentials, _ = google.auth.default()
    service = build('dataflow', 'v1b3', credentials=credentials)
    
    template_path = 'gs://' + 'temp_crypto_batch_' + google.auth.default()[1].replace('-','_') \
    + '/prediction/templates/CUSTOM_TEMPLATE_DATAFLOW'
    
    template_body = {
        "jobName": ("testing-dataflow_prophet_from"),
    }
    
    request = service.projects().templates().launch(
                    projectId=google.auth.default()[1],
                    gcsPath=template_path,
                    body=template_body
    )
    
    response = request.execute()
    
    logging.info(f'response status: {response}')