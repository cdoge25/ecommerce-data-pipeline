from minio import Minio

from airflow.hooks.base import BaseHook


def _get_minio_client():
    airflow_minio_connection = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=airflow_minio_connection.extra_dejson["endpoint_url"].split("//")[1],
        access_key=airflow_minio_connection.login,
        secret_key=airflow_minio_connection.password,
        secure=False,
    )
    return client
