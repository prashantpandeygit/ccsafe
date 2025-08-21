from datetime import datetime, timedelta
import uuid
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
from dotenv import load_dotenv

load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 30),
}

with DAG(
    dag_id="transactions_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    gcs_bucket = os.getenv("GCS_BUCKET")
    file_pattern = os.getenv("FILE_PATTERN")
    source_prefix = os.getenv("SOURCE_PREFIX")
    archive_prefix = os.getenv("ARCHIVE_PREFIX")

    file_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id="check_json_file_arrival",
        bucket=gcs_bucket,
        prefix=file_pattern,
        timeout=600,
        poke_interval=30,
        mode="poke",
    )

    batch_id = f"credit-card-batch-{str(uuid.uuid4())[:8]}"

    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": os.getenv("MAIN_PYSPARK_URI"),
        },
        "runtime_config": {
            "version": os.getenv("DATAPROC_VERSION", "2.2"),
        },
        "environment_config": {
            "execution_config": {
                "service_account": os.getenv("SERVICE_ACCOUNT"),
                "network_uri": os.getenv("NETWORK_URI"),
                "subnetwork_uri": os.getenv("SUBNETWORK_URI"),
            }
        },
    }

    pyspark_task = DataprocCreateBatchOperator(
        task_id="cc_processing",
        batch=batch_details,
        batch_id=batch_id,
        project_id=os.getenv("PROJECT_ID"),
        region=os.getenv("REGION"),
        gcp_conn_id=os.getenv("GCP_CONN_ID", "google_cloud_default"),
    )

    move_files_to_archive = GCSToGCSOperator(
        task_id="move_files_to_archive",
        source_bucket=gcs_bucket,
        source_object=source_prefix,
        destination_bucket=gcs_bucket,
        destination_object=archive_prefix,
        move_object=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    file_sensor >> pyspark_task >> move_files_to_archive
