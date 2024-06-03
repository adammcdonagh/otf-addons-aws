"""Caching plugin for writing variables to AWS SSM."""

import os

import boto3
import opentaskpy.otflogging
from botocore.exceptions import ClientError
from opentaskpy.exceptions import CachingPluginError

logger = opentaskpy.otflogging.init_logging(__name__)

CACHE_NAME = "vc_ssm"


def run(**kwargs):  # type: ignore[no-untyped-def]
    """Write a variable to AWS SSM.

    Args:
        **kwargs: Expect kwargs named 'name', and 'value'. This should be the Parameter
         Store parameter value to write to, and the value to put into the file

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
        ssm = session.client("ssm", **kwargs2)

        # Write the value to the SSM parameter
        ssm.put_parameter(
            Name=kwargs["name"],
            Value=kwargs["value"],
            Type="SecureString",
            Overwrite=True,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ParameterNotFound":
            logger.error(f"Parameter not found: {kwargs['name']}: {e}")
            raise e
        raise e
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(f"Failed to write SSM parameter: {kwargs['name']}: {e}")
        raise e
