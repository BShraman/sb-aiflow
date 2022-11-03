'''
  Dynamically Creating Airflow Dags
'''

import os
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from google.cloud import storage

def getGCSFiles(bucket_name):
    list_of_dicts = []
    client = storage.Client()
    for blob in client.list_blobs(bucket_name,prefix='landing'):
        file_name = str(blob).split(",")
        file_name = file_name[1].strip()

        list_of_dicts.append({"blob_name" : file_name})
    
    return list_of_dicts

def create_dag(dag_id, 
                schedule,
                task_name,
                default_args):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)
    
    with dag:

        #Copy file in to Processing Folder
        @task
        def copyGCSFiles(blob_name):
            storage_client = storage.Client()

            source_bucket = storage_client.bucket(bucket_name)
            source_blob = source_bucket.blob(blob_name)
            destination_bucket = storage_client.bucket(bucket_name)
            destination_blob_name = 'processing/'+blob_name.split('/')[1] +'/'+blob_name.split('/')[2]

            source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)

            #Removing File from bucket
            blob = source_bucket.blob(blob_name)
            blob.delete()

        start = EmptyOperator(
            task_id='start'
            )

        end = EmptyOperator(
            task_id='end'
            )

        start >> copyGCSFiles(blob_name=task_name) >> end
    
    return dag

gcs_bucket_name = os.environ.get("LANDING_BUCKET") 
bucket_name = gcs_bucket_name.replace("gs://", "").strip("/")

files = getGCSFiles(bucket_name)

current_date_string = datetime.now().strftime("%Y,%m,%d").split(',')

for file in files:
   blob = str(file['blob_name']).split("/")
   blob = str(blob[2]).split(".")
   dag_name = blob[0].strip()

   dag_id="dynamic_dag_{}".format(dag_name)
   task_name = file['blob_name']

   default_args = {'owner': 'airflow',
                    'start_date': datetime((int(current_date_string[0])), int((current_date_string[1])),
                                           int((current_date_string[2]))),
                    }
                    
   schedule = '@once'

   globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  task_name,
                                  default_args)
