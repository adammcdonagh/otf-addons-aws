"""Caching plugin for writing variables to AWS Secrets Manager."""

import os
from datetime import datetime, timedelta

import boto3
import opentaskpy.otflogging
from botocore.exceptions import ClientError
from dateutil.tz import tzlocal
from opentaskpy.exceptions import CachingPluginError

logger = opentaskpy.otflogging.init_logging(__name__)

CACHE_NAME = "vc_secretsmanager"


def run(**kwargs):  # type: ignore[no-untyped-def]
    """Write a variable to AWS Secrets Manager.

    Args:
        **kwargs: Expect kwargs named 'name', and 'value'. This should be the Secrets
         Manager secret to write to, and the value to put into the file.
         Optional kwargs named 'min_cache_age'. This should be the minimum amount of time (in seconds) to wait before saving this variable in secrets manager again
         (recommended > 10 mins to avoid possible issues with running out of secret versions)

    Raises:
        CachingPluginError: Returned if the kwarg 'name' or 'value' is not provided
        FileNotFoundError: Returned if the file does not exist
    """
    # Expect a kwarg named name, and value
    expected_kwargs = ["name", "value"]
    for kwarg in expected_kwargs:
        if kwarg not in kwargs:
            raise CachingPluginError(
                f"Missing kwarg: '{kwarg}' while trying to run caching plugin"
                f" '{CACHE_NAME}'"
            )

    globals_ = kwargs.get("globals", None)

    aws_access_key_id = (
        globals_["AWS_ACCESS_KEY_ID"]
        if globals_ and "AWS_ACCESS_KEY_ID" in globals_
        else os.environ.get("AWS_ACCESS_KEY_ID")
    )
    aws_secret_access_key = (
        globals_["AWS_SECRET_ACCESS_KEY"]
        if globals_ and "AWS_SECRET_ACCESS_KEY" in globals_
        else os.environ.get("AWS_SECRET_ACCESS_KEY")
    )

    region_name = (
        globals_["AWS_REGION"]
        if globals_ and "AWS_REGION" in globals_
        else os.environ.get("AWS_REGION")
    )
    boto3_kwargs = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "region_name": region_name,
    }
    # If there's an override for endpoint_url in the environment, then use that
    kwargs2 = {}
    if os.environ.get("AWS_ENDPOINT_URL"):
        kwargs2["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL")

    try:
        session = boto3.session.Session(**boto3_kwargs)
        secrets_manager = session.client("secretsmanager", **kwargs2)

        # Check if this secret has been updated more recently than min cache age (if applicable)
        if "min_cache_age" in kwargs:
            secret = secrets_manager.describe_secret(
                SecretId=kwargs["name"],
            )
            if secret["LastChangedDate"] > datetime.now(tz=tzlocal()) - timedelta(
                seconds=int(kwargs["min_cache_age"])
            ):
                logger.warning(
                    f"Not updating secret because secret last updated at {secret['LastChangedDate']}, and min_cache_age is {kwargs['min_cache_age']}"
                )
                return

        # Write the value to secrets manager
        secrets_manager.put_secret_value(
            SecretId=kwargs["name"],
            SecretString=kwargs["value"],
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.error(f"Secret not found: {kwargs['name']}: {e}")
            raise e
        raise e
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(f"Failed to write secret: {kwargs['name']}: {e}")
        raise e
