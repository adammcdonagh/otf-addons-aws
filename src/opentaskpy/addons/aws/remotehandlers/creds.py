"""AWS helper functions."""

import os
from time import time

import boto3
import opentaskpy.otflogging

logger = opentaskpy.otflogging.init_logging(__name__, None, None)


def get_aws_client(
    client_type: str, credentials: dict, assume_role_arn: str | None = None
) -> dict:
    """Get an AWS client of the specified type using the provided credentials.

    Args:
        client_type: The type of client to get
        credentials: The credentials to use
        assume_role_arn: The role to assume
    """
    supported_types = ["s3", "ecs", "lambda", "logs"]
    if client_type not in supported_types:
        raise ValueError(
            f"Unsupported client type: {client_type}. Supported types are: {supported_types}"
        )

    kwargs = {}
    if os.environ.get("AWS_ENDPOINT_URL"):
        kwargs["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL")

    if assume_role_arn:
        logger.info(f"Assuming role: {assume_role_arn}")
        sts_client = boto3.client("sts", **kwargs)

        assumed_role_object = sts_client.assume_role(
            RoleArn=assume_role_arn,
            RoleSessionName=f"OTF{time()}",
            DurationSeconds=900,
        )

        credentials = assumed_role_object["Credentials"]
        # Log the assumed role access key id
        logger.info(f"Assumed role access key id: {credentials['AccessKeyId']}")

    kwargs2 = {
        "aws_access_key_id": credentials["AccessKeyId"],
        "aws_secret_access_key": credentials["SecretAccessKey"],
    }
    if "SessionToken" in credentials:
        kwargs2["aws_session_token"] = credentials["SessionToken"]

    if "region_name" in credentials:
        kwargs2["region_name"] = credentials["region_name"]

    session = boto3.session.Session(**kwargs2)

    return {
        "client": session.client(client_type, **kwargs),
        "temporary_creds": credentials if assume_role_arn else None,
    }


def set_aws_creds(obj) -> None:  # type: ignore[no-untyped-def]
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

    obj.assume_role_arn = (
        obj.spec["protocol"]["assume_role_arn"]
        if "assume_role_arn" in obj.spec["protocol"]
        else os.environ.get("AWS_ROLE_ARN")
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
