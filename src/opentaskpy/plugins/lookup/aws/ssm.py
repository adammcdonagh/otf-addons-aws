"""SSM Param Store lookup plugin.

Uses boto3 to pull out a parameter from AWS Parameter Store
This uses AWS authentication, either from the IAM role of the host,
by using environment variables, or variables in variables.json file
"""

import json
import os

import boto3
import opentaskpy.otflogging
from botocore.exceptions import ClientError
from opentaskpy.exceptions import LookupPluginError

logger = opentaskpy.otflogging.init_logging(__name__)

plugin_name = "ssm"


def run(**kwargs):  # type: ignore[no-untyped-def]
    """Pull a variable from AWS SSM.

    Args:
        **kwargs: Expect a kwarg named name. This should be the key within SSM to the
        variable to obtain. The value should be a string.

    Raises:
        LookupPluginError: Returned if the kwarg 'name' is not provided
        ClientError: Returned if the parameter does not exist

    Returns:
        _type_: The value read from Parameter Store
    """
    # Expect a kwarg named name
    expected_kwargs = ["name"]
    for kwarg in expected_kwargs:
        if kwarg not in kwargs:
            raise LookupPluginError(
                f"Missing kwarg: '{kwarg}' while trying to run lookup plugin"
                f" '{plugin_name}'"
            )

    fail_on_exception = (
        os.environ.get("OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR", "0") == "1"
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

    result = None
    try:
        session = boto3.session.Session(**boto3_kwargs)
        ssm = session.client("ssm", **kwargs2)
        response = ssm.get_parameter(Name=kwargs["name"], WithDecryption=True)
        result = response["Parameter"]["Value"]

        # Very simple redacting filter here for secrets, e.g. pgp private keys,
        # or ssh private keys
        log_result = result
        if " private " in result.lower():
            log_result = "REDACTED"

        logger.log(12, f"Read '{log_result}' from param {kwargs['name']}")

    except ClientError as e:
        # To prevent complete failure of all jobs in an environment on failed lookup return 'LOOKUP_FAILED and log error instead of throwing exception'
        if e.response["Error"]["Code"] == "ParameterNotFound":
            logger.error(f"Parameter not found: {kwargs['name']}: {e}")
        else:
            logger.error(f"Failed to read from SSM parameter: {kwargs['name']}: {e}")

        if fail_on_exception:
            raise e

        logger.warning("SSM parameter lookup failed but continuing anyway")
        return "LOOKUP_FAILED"
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(f"Failed to read from SSM parameter: {kwargs['name']}: {e}")

        if fail_on_exception:
            raise e

        logger.warning("SSM parameter lookup failed but continuing anyway")
        return "LOOKUP_FAILED"

    # Escape any escape characters so they can be stored in JSON as a string
    if result:
        # Escape any newline characters
        result = result.replace("\n", "\\n")
        result = json.dumps(result)
        # Remove the leading and trailing quotes
        result = result[1:-1]

    return result
