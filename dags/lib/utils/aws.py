import os
from urllib.parse import parse_qs, urlparse
import dotenv
import boto3

dotenv.load_dotenv()
def get_s3_client():
    conn_uri = os.getenv("AIRFLOW_CONN_AWS_S3")
    if not conn_uri:
        raise ValueError("AIRFLOW_CONN_AWS_S3 is missing from the environment.")

    parsed = urlparse(conn_uri)
    query_params = parse_qs(parsed.query)
    endpoint = query_params.get("endpoint_url", [None])[0]

    return boto3.client(
        "s3",
        aws_access_key_id=parsed.username,
        aws_secret_access_key=parsed.password,
        endpoint_url=endpoint,
    )
