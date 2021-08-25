from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.gapic.transports import (
    job_controller_grpc_transport,
    cluster_controller_grpc_transport,
)
import google.protobuf
import google.auth

# parameters
project = google.auth.default()[1] # GCP project name 
cluster_name = 'sparkviacf' # Dataproc cluster name
python_file = 'gs://' + 'temp_crypto_batch_' + google.auth.default()[1].replace('-','_') + '/prediction/spark_prophet_ml.py' 
# where your script is, in a gs bucket usually
region = 'us-central1'
zone = 'us-central1-a'
worker_config = 2 # number of workers

# gcloud functions deploy trigger_spark_job --runtime python37 \
# --trigger-resource gs://{insert-your-trigger-bucket-here} \
# --trigger-event google.storage.object.finalize --timeout=300
def trigger_spark_job(event=None, context=None):
    
    duration_message = google.protobuf.duration_pb2.Duration()
    duration_message.FromSeconds(600)
    
    # 1. Create a cluster
    client_transport = (
        cluster_controller_grpc_transport.ClusterControllerGrpcTransport(
            address='{}-dataproc.googleapis.com:443'.format(region)))
    
    dataproc_cluster_client = dataproc_v1.ClusterControllerClient(
            client_transport)
    
    zone_uri = 'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(project, zone)
    
    cluster_data = {
        'project_id': project,
        'cluster_name': cluster_name,
        'config': {
            'gce_cluster_config': {
                'zone_uri': zone_uri
            },
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'n1-standard-2', 
                "disk_config": {
                                "boot_disk_size_gb": 250,
                                "boot_disk_type": "pd-standard"
                }
            },
            'worker_config': {
                'num_instances': worker_config,
                'machine_type_uri': 'n1-standard-2', 
                "disk_config": {
                "boot_disk_size_gb": 250,
                "boot_disk_type": "pd-standard"
            }},
            'software_config': {
                'properties': {"dataproc:pip.packages": "pystan==2.19.1.1,fbprophet==0.7.1"},
                'optional_components': [
                    'JUPYTER'
                ]
            },
             "lifecycle_config": {
                  "idle_delete_ttl": duration_message
            },
        }
    }
    
    cluster = dataproc_cluster_client.create_cluster(project, region, cluster_data)
    
    # NOTE: CLUSTER MUST BE CREATED THEN ONLY WE CAN SUBMIT JOB
    cluster.add_done_callback(lambda _: submit_job())
        
def submit_job():

    job_transport = (
        job_controller_grpc_transport.JobControllerGrpcTransport(
            address='{}-dataproc.googleapis.com:443'.format(region)))

    dataproc = dataproc_v1.JobControllerClient(job_transport)

    job_details = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': python_file, 
            'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
        }
    }

    result = dataproc.submit_job(
        project_id=project, region=region, job=job_details)
    job_id = result.reference.job_id

    print('Submitted job ID {}.'.format(job_id))
    