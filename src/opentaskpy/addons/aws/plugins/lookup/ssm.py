"""
SSM Param Store lookup plugin

Uses boto3 to pull out a parameter from AWS Parameter Store
This uses AWS authentication, either from the IAM role of the host,
by using environment variables, or variables in variables.json file

"""
import os

import boto3
import opentaskpy.logging
from botocore.exceptions import ClientError

logger = opentaskpy.logging.init_logging(__name__)

plugin_name = "ssm"


def run(**kwargs):
    # Expect a kwarg named url, and value
    expected_kwargs = ["name"]
    for kwarg in expected_kwargs:
        if kwarg not in kwargs:
            raise Exception(
                f"Missing kwarg: '{kwarg}' while trying to run lookup plugin '{plugin_name}'"
            )

    globals = kwargs["globals"] if "globals" in kwargs else None

    aws_access_key_id = (
        globals["AWS_ACCESS_KEY_ID"]
        if globals and "AWS_ACCESS_KEY_ID" in globals
        else os.environ.get("AWS_ACCESS_KEY_ID")
    )
    aws_secret_access_key = (
        globals["AWS_SECRET_ACCESS_KEY"]
        if globals and "AWS_SECRET_ACCESS_KEY" in globals
        else os.environ.get("AWS_SECRET_ACCESS_KEY")
    )

    region_name = (
        globals["AWS_REGION"]
        if globals and "AWS_REGION" in globals
        else os.environ.get("AWS_REGION")
    )
    boto3_kwargs = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "region_name": region_name,
    }
    # If there's an override for endpoint_url in the environment, then use that
    if os.environ.get("AWS_ENDPOINT_URL"):
        boto3_kwargs["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL")

    result = None
    try:
        ssm = boto3.client("ssm", **boto3_kwargs)
        response = ssm.get_parameter(Name=kwargs["name"], WithDecryption=True)
        result = response["Parameter"]["Value"]

        logger.log(12, f"Read '{result}' from param {kwargs['name']}")

    except ClientError as e:
        if e.response["Error"]["Code"] == "ParameterNotFound":
            logger.error(f"Parameter not found: {kwargs['name']}: {e}")
            raise e
        else:
            raise e
    except Exception as e:
        logger.error(f"Failed to read from SSM parameter: {kwargs['name']}: {e}")

    return result
