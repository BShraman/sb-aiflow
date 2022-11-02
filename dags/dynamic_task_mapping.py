'''
   Mapping task dynamically using airflow functional API in Google Cloud Storage as source. 
   Tested in Airflow 2.4.2
'''

import os
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from google.cloud import storage

gcs_bucket_name = os.environ.get("LANDING_BUCKET") 
bucket_name = gcs_bucket_name.replace("gs://", "").strip("/")

dag_id='Dynamic_Task_Mapping'
with DAG(dag_id=dag_id, 
        start_date=datetime(2022, 10, 17),
        catchup=False,
        schedule='@daily') as dag:

    with TaskGroup(group_id='GCSArtifacts') as GCSArtifacts:
        # Get the List of file from Storage Bucket
        @task
        def getGCSFiles(bucket_name):
            list_of_dicts = []
            client = storage.Client()
            for blob in client.list_blobs(bucket_name,prefix='landing'):
                file_name = str(blob).split(",")
                file_name = file_name[1].strip()

                list_of_dicts.append({"blob_name" : file_name})
    
            return list_of_dicts

        #Copy file in to Processing Folder
        @task
        def copyGCSFiles(blob_name):
            storage_client = storage.Client()

            source_bucket = storage_client.bucket(bucket_name)
            source_blob = source_bucket.blob(blob_name)
            destination_bucket = storage_client.bucket(bucket_name)
            destination_blob_name = 'processing/'+blob_name.split('/')[1] +'/'+blob_name.split('/')[2]

            blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)

            #Removing File from bucket
            blob = source_bucket.blob(blob_name)
            blob.delete()

        copyGCSFiles.expand_kwargs(getGCSFiles(bucket_name))
    
    gcs_object_exists = GCSObjectsWithPrefixExistenceSensor(
        task_id="gcs_object_exists",
        bucket=bucket_name,
        prefix='landing',
        poke_interval=30,
        timeout=60 *5,
        mode='reschedule'
    )

    trigger_same_dag = TriggerDagRunOperator(
        task_id='trigger_same_dag',
        trigger_dag_id=dag_id,
        execution_date='{{ ds }}',
        reset_dag_run=True
    )

    gcs_object_exists >> GCSArtifacts >> trigger_same_dag
