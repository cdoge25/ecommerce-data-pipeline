import io

import requests
from scripts.connectors import _get_minio_client

RAW_BUCKET = "raw"
PROCESSED_BUCKET = "processed"


def _get_google_drive_file_list(folder_id: str, api_key: str) -> list:
    response = requests.get(
        f"https://www.googleapis.com/drive/v3/files?key={api_key}&q='{folder_id}' in parents&fields=files(id, name)&key={api_key}",
    )
    return response.json()["files"]


def _get_google_drive_file_content(file_id: str, api_key: str) -> bytes:
    response = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{file_id}?key={api_key}&alt=media",
    )
    return response.content


def _prepare_minio_buckets():
    minio_client = _get_minio_client()
    if not minio_client.bucket_exists(RAW_BUCKET):
        minio_client.make_bucket(RAW_BUCKET)
    if not minio_client.bucket_exists(PROCESSED_BUCKET):
        minio_client.make_bucket(PROCESSED_BUCKET)


def _ingest_to_minio(object_name: str, data: bytes) -> str:
    minio_client = _get_minio_client()
    data_stream = io.BytesIO(data)
    minio_client.put_object(
        bucket_name=RAW_BUCKET,
        object_name=object_name,
        data=data_stream,
        length=len(data),
    )
    file_path = f"{RAW_BUCKET}/{object_name}"
    return file_path


# def _ingest_to_azure(object_name: str, data: bytes) -> str:
#     import pandas as pd

#     df = pd.read_csv(io.BytesIO(data))
#     df.to_csv(
#         "abfs://ecommerce@haindt.dfs.core.windows.net/raw/" + object_name,
#         storage_options={
#             "account_key": ""
#         },
#     )

#     return
