from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.operators.bash import BashOperator
import boto3

# Param?tres MinIO
MINIO_ENDPOINT = "http://host.docker.internal:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
RAW_BUCKET = "raw-data"
PROCESSED_BUCKET = "processed-data"

def check_new_csv(**kwargs):
    """V?rifie si un fichier CSV existe dans le bucket raw-data."""
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    response = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix='', MaxKeys=10)
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            print(f"Fichier CSV trouv? : {obj['Key']}")
            return True
    return False

default_args = {
    'owner': 'ilyass',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'local_data_lake_pipeline',
    default_args=default_args,
    description='Pipeline ETL avec surveillance MinIO',
    schedule='@hourly',
    catchup=False,
    tags=['data-engineering'],
) as dag:

    # T?che 1 : Attendre un fichier CSV dans MinIO
    wait_for_csv = PythonSensor(
        task_id='wait_for_csv',
        python_callable=check_new_csv,
        timeout=60*60,          # attendre jusqu'? 1 heure
        poke_interval=60,       # v?rifier toutes les 60 secondes
        mode='poke'             # mode classique
    )

    # T?che 2 : Ex?cuter le script ETL
    run_etl = BashOperator(
        task_id='run_etl_script',
        bash_command='cd /opt/airflow/dags && python etl_minio.py',
    )

    wait_for_csv >> run_etl
