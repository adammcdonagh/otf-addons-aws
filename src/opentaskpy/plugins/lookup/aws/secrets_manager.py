"""Secrets Manager lookup plugin.

Uses boto3 to pull out a secret from AWS Secrets Manager
This uses AWS authentication, either from the IAM role of the host,
by using environment variables, or variables in variables.json file
"""

import json
import os

import boto3
import opentaskpy.otflogging
from botocore.exceptions import ClientError
from jsonpath_ng import parse
from opentaskpy.exceptions import LookupPluginError

logger = opentaskpy.otflogging.init_logging(__name__)

plugin_name = "secretsmanager"


def run(**kwargs):  # type: ignore[no-untyped-def]
    """Pull a secret from AWS Secrets Manager.

    Args:
        **kwargs: Expect a kwarg named name. This should be the secret name
        to obtain. The value should be a string.
          Optionally, a kwarg named value pointing at the JSON path, if the secret is to
            be parsed as JSON. The value returned will be the value at the path. The
            value should be a string.


    Raises:
        LookupPluginError: Returned if the kwarg 'name' is not provided, or if the JSONPath
            returns a value of the wrong type
        ClientError: Returned if anything fails during the lookup

    Returns:
        _type_: The value read from Parameter Store. If there is an issue obtaining the
            value, then "LOOKUP_FAILED" is returned, instead of causing the whole task
            to fail, unless the environment variable OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR
            is set to 1.
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
        secretsmanager = session.client("secretsmanager", **kwargs2)
        response = secretsmanager.get_secret_value(SecretId=kwargs["name"])
        result = response["SecretString"]

        # Very simple redacting filter here for secrets, e.g. pgp private keys,
        # or ssh private keys
        log_result = result
        if " private " in result.lower():
            log_result = "REDACTED"

        logger.log(12, f"Read '{log_result}' from param {kwargs['name']}")

        # If requested to pull a value from the JSON, then parse it and extract the
        # value at the path
        if "value" in kwargs:
            try:
                result = json.loads(result)
                # Handle the JSONPath
                jsonpath_expr = parse(kwargs["value"])
                result = jsonpath_expr.find(result)

                result = result[0].value

                # If the result is a list, then return the first element, with a warning
                # that there's more than one
                if isinstance(result, list) and isinstance(result[0], (str, int)):
                    logger.warning(
                        f"JSONPath returned a list of length {len(result)}. Returning "
                        + "only the first element"
                    )
                    result = result[0]

                # If the result is not a string or an int, then raise an exception
                if not isinstance(result, (str, int)):
                    if fail_on_exception:
                        raise LookupPluginError(
                            f"JSONPath returned a value of type {type(result)}. Expected a string or int"
                        )
                    logger.warning(
                        f"JSONPath returned a value of type {type(result)}. Continuing anyway."
                    )
                    return "LOOKUP_FAILED"

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON from secret: {kwargs['name']}: {e}")

                if fail_on_exception:
                    raise e

                logger.warning("Secret JSON parsing failed but continuing anyway")
                return "LOOKUP_FAILED"
            except KeyError as e:
                logger.error(f"Failed to read from secret: {kwargs['name']}: {e}")

                if fail_on_exception:
                    raise e

                logger.warning("Secret JSON parsing failed but continuing anyway")
                return "LOOKUP_FAILED"

    except ClientError as e:
        # To prevent complete failure of all jobs in an environment on failed lookup
        # return 'LOOKUP_FAILED and log error instead of throwing exception'
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.error(f"Secret not found: {kwargs['name']}: {e}")
        else:
            logger.error(f"Failed to read from Secrets Manager: {kwargs['name']}: {e}")

        if fail_on_exception:
            raise e

        logger.warning("Secrets Manager lookup failed but continuing anyway")
        return "LOOKUP_FAILED"
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(f"Failed to read from Secrets Manager: {kwargs['name']}: {e}")

        if fail_on_exception:
            raise e

        logger.warning("Secrets Manager lookup failed but continuing anyway")
        return "LOOKUP_FAILED"

    # Escape any escape characters so they can be stored in JSON as a string
    if result and isinstance(result, str):
        # Escape any newline characters
        result = result.replace("\n", "\\n")
        result = json.dumps(result)
        # Remove the leading and trailing quotes
        result = result[1:-1]

    return result
