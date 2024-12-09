import os
from datetime import datetime

from scripts.extractors import (
    _get_google_drive_file_content,
    _get_google_drive_file_list,
    _ingest_to_minio,
    _prepare_minio_buckets,
)
from scripts.processors.minio_processor import MinioCSVFileProcessor

from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

MINIO_ACCESS_KEY_ID = os.getenv("MINIO_ACCESS_KEY_ID")
MINIO_SECRET_ACCESS_KEY = os.getenv("MINIO_SECRET_ACCESS_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")


@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["extract"],
)
def etl_ecom():
    @task
    def get_google_drive_file_list():
        file_list = _get_google_drive_file_list(
            folder_id=GOOGLE_DRIVE_FOLDER_ID, api_key=GOOGLE_API_KEY
        )
        return file_list

    @task
    def prepare_minio_buckets():
        _prepare_minio_buckets()

    @task
    def ingest_file(file):
        file_content = _get_google_drive_file_content(
            file_id=file["id"], api_key=GOOGLE_API_KEY
        )
        file_path = _ingest_to_minio(object_name=file["name"], data=file_content)
        return file_path

    @task
    def process_file(file_path):
        processor = MinioCSVFileProcessor()
        processed_file_path = processor._process_file(file_path=file_path)
        return processed_file_path

    @task
    def aggregate_processed_files(processed_files):
        return processed_files

    load_to_dw = DockerOperator(
        task_id="load_to_dw",
        image="spark-app",
        api_version="auto",
        auto_remove=True,
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "MINIO_ACCESS_KEY_ID": MINIO_ACCESS_KEY_ID,
            "MINIO_SECRET_ACCESS_KEY": MINIO_SECRET_ACCESS_KEY,
            "MINIO_ENDPOINT": MINIO_ENDPOINT,
            "POSTGRES_HOST": POSTGRES_HOST,
            "POSTGRES_PORT": POSTGRES_PORT,
            "POSTGRES_DATABASE": POSTGRES_DATABASE,
            "POSTGRES_SCHEMA": POSTGRES_SCHEMA,
            "POSTGRES_USER": POSTGRES_USER,
            "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
            "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
            "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
            "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
            "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
            "SNOWFLAKE_USER": SNOWFLAKE_USER,
            "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
            "SNOWFLAKE_ROLE": SNOWFLAKE_ROLE,
            "PROCESSED_FILE_PATH_LIST_STR": "{{ task_instance.xcom_pull(task_ids='aggregate_processed_files') }}",
        },
    )

    file_list = get_google_drive_file_list()
    ingested_file_list = ingest_file.expand(file=file_list)
    processed_file_list = process_file.expand(file_path=ingested_file_list)
    aggregated_file_list = aggregate_processed_files(
        processed_files=processed_file_list
    )

    (
        file_list
        >> prepare_minio_buckets()
        >> ingested_file_list
        >> processed_file_list
        >> aggregated_file_list
        >> load_to_dw
    )


etl_ecom()
