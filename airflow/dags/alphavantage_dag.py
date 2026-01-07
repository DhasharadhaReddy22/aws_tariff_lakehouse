import os
import sys
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

sys.path.append('/opt/airflow')
from src.ingestion.imf_datamapper import fetch_imf_indicators_raw
from src.utils.bucket import BucketClient

logger = logging.getLogger(__name__)

default_args = {
    "owner": "dasarath",
    # "retries": 2,
    # "retry_delay": timedelta(minutes=2),
}

def fetch_api_data(indicators, params, **kwargs):
    logger.info(f"Fetching IMF data for indicators={indicators}, params={params}")
    records = fetch_imf_indicators_raw(indicators, params=params)
    return records

def write_to_minio(prefix, filename, **kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids="imf_ingestion.fetch_api_data")
    if not records:
        raise ValueError("No records fetched from IMF API!")

    datasets = BucketClient(
        bucket_name="dataset-files",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
    )
    datasets.append_s3(prefix, filename, records)
    logger.info(f"Successfully wrote {len(records)} records to {prefix}/{filename}")

# Variables, the same can be done as a json import on Airflow CLI through CI/CD or Airflow UI Admin/Variables 
Variable.set("imf_indicators", '["NGDP_RPCH", "NGDPD"]')
Variable.set("imf_params", '{"years": [2022, 2023, 2024], "countries": ["IND", "USA"]}')

with DAG(
    dag_id="imf_pipeline",
    default_args=default_args,
    description="IMF Data Ingestion Pipeline",
    start_date=datetime(2025, 9, 20),
    schedule=timedelta(minutes=2),
    catchup=False,
    tags=["imf", "data_ingestion", "minio"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup("imf_ingestion") as imf_ingestion:
        fetch_api = PythonOperator(
            task_id="fetch_api_data",
            python_callable=fetch_api_data,
            op_kwargs={
                "indicators": Variable.get("imf_indicators", deserialize_json=True),
                "params": Variable.get("imf_params", deserialize_json=True),
            }
        )

        write_s3 = PythonOperator(
            task_id="write_to_minio",
            python_callable=write_to_minio,
            op_kwargs={
                "prefix": "raw/imf",
                "filename": "indicators_2.jsonl"
            }
        )
        
        fetch_api >> write_s3
    
    with TaskGroup("imf_dbt_transforms") as imf_dbt_transforms:
        bronze_imf = DockerOperator( # spins up an ephemeral container
            task_id="raw_to_bronze",
            image="dbt-dremio:custom",
            api_version="auto",
            auto_remove='success',
            command="dbt run -s bronze_imf",
            docker_url="unix://var/run/docker.sock",
            network_mode="tariffs_pipeline_pipeline-network",
            working_dir="/usr/app",
            mount_tmp_dir=False,
            mounts=[ # since not temporary mount, we need to bind it?
                Mount(
                    source="/home/dhasharadhareddyb/projects/tariffs_pipeline/dbt/datalake", 
                    target="/usr/app", 
                    type="bind"
                ),
                Mount(
                    source="/home/dhasharadhareddyb/projects/tariffs_pipeline/dbt/profiles.yml", 
                    target="/home/dbt_user/.dbt/profiles.yml", 
                    type="bind"
                ),
            ],
            environment={
                "DBT_PROFILES_DIR": "/home/dbt_user/.dbt"
            },
        )

        silver_imf = DockerOperator(
            task_id="bronze_to_silver",
            image="dbt-dremio:custom",
            api_version="auto",
            auto_remove='success',
            command="dbt run -s silver_imf",
            docker_url="unix://var/run/docker.sock",
            network_mode="tariffs_pipeline_pipeline-network",
            working_dir="/usr/app",
            mount_tmp_dir=False,
            mounts=[
                Mount(
                    source="/home/dhasharadhareddyb/projects/tariffs_pipeline/dbt/datalake", 
                    target="/usr/app", 
                    type="bind"
                ),
                Mount(
                    source="/home/dhasharadhareddyb/projects/tariffs_pipeline/dbt/profiles.yml", 
                    target="/home/dbt_user/.dbt/profiles.yml", 
                    type="bind"
                ),
            ],
            environment={
                "DBT_PROFILES_DIR": "/home/dbt_user/.dbt"
            },
        )

        bronze_imf >> silver_imf

    stop = EmptyOperator(task_id="stop")

    start >> imf_ingestion >> imf_dbt_transforms >> stop