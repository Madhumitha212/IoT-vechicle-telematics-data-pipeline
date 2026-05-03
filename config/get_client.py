import boto3
import os
from dotenv import load_dotenv

load_dotenv()

REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE")
REDSHIFT_DB_USER = os.getenv("REDSHIFT_DB_USER")
REDSHIFT_WORKGROUP = os.getenv("REDSHIFT_WORKGROUP")
REDSHIFT_IAM_ROLE_ARN = os.getenv("REDSHIFT_IAM_ROLE_ARN")

AWS_REGION = os.getenv("AWS_REGION")
BASE_S3 = os.getenv("BASE_S3")

_redshift_client = None

def get_redshift_client():
    global _redshift_client
    if _redshift_client is None:
        _redshift_client = boto3.client(
            "redshift-data",
            region_name=AWS_REGION
        )
    return _redshift_client
