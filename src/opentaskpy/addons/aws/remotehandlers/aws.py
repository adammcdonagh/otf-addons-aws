"""AWS helper functions."""
import os

from .s3 import S3Execution, S3Transfer


def set_aws_creds(obj: S3Transfer | S3Execution) -> None:
    """Set AWS credentials for boto3.

    Args:
        obj: The object to set the credentials on

    """
    obj.aws_access_key_id = (
        obj.spec["protocol"]["access_key_id"]
        if "access_key_id" in obj.spec["protocol"]
        else os.environ.get("AWS_ACCESS_KEY_ID")
    )
    obj.aws_secret_access_key = (
        obj.spec["protocol"]["secret_access_key"]
        if "secret_access_key" in obj.spec["protocol"]
        else os.environ.get("AWS_SECRET_ACCESS_KEY")
    )

    obj.region_name = (
        obj.spec["protocol"]["region_name"]
        if "region_name" in obj.spec["protocol"]
        else os.environ.get("AWS_REGION")
    )

    obj.bucket_owner_full_control = (
        obj.spec["protocol"]["bucket_owner_full_control"]
        if "bucket_owner_full_control" in obj.spec["protocol"]
        else True
    )
