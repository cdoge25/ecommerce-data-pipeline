import os
from datetime import datetime

from airflow.decorators import dag, task
from include.scripts.connectors import _get_minio_client
from include.scripts.extractors import (
    _get_google_drive_file_content,
    _get_google_drive_file_list,
    _ingest_to_minio,
    _prepare_minio_buckets,
)
from include.scripts.processors.minio_processor import MinioCSVFileProcessor

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")


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

    file_list = get_google_drive_file_list()
    ingested_file_list = ingest_file.expand(file=file_list)
    processed_file_list = process_file.expand(file_path=ingested_file_list)

    file_list >> prepare_minio_buckets() >> ingested_file_list >> processed_file_list


etl_ecom()
