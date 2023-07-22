# pylint: skip-file
import io
import json
import logging
import os
import re
import subprocess
import time
import zipfile
from contextlib import suppress

import botocore.exceptions
import opentaskpy.otflogging
import pytest
from opentaskpy.config.loader import ConfigLoader
from opentaskpy.taskhandlers import batch, execution
from pytest_shell import fs

from tests.fixtures.localstack import *  # noqa: F403, F405

os.environ["OTF_LOG_LEVEL"] = "DEBUG"

BUCKET_NAME = "otf-addons-aws-lambda-execution-test"
BUCKET_NAME_1 = "otf-addons-aws-lambda-execution-test-1"
BUCKET_NAME_2 = "otf-addons-aws-lambda-execution-test-2"
logger = opentaskpy.otflogging.init_logging(__name__)

logger.setLevel(logging.DEBUG)

root_dir_ = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "test",
)

lambda_execution_task_definition = {
    "type": "execution",
    "functionArn": None,
    "payload": {"bucket_name": BUCKET_NAME, "file_name": "function_test.txt"},
    "protocol": {
        "name": "opentaskpy.addons.aws.remotehandlers.lambda.LambdaExecution",
    },
}

lambda_batch_task_definition = {
    "type": "batch",
    "tasks": [
        {
            "order_id": 1,
            "task_id": "lambda_execution_task_definition",
            "timeout": 2,
        }
    ],
}


def create_lambda_function(lambda_client, lambda_handler, payload, invoke=True):
    # Read in lambda_handler
    lambda_code = None
    with open(os.path.join(root_dir_, "../tests", lambda_handler), "rb") as f:
        lambda_code = f.read()

    zip_buffer = io.BytesIO()
    # Write a zip file into the buffer
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        info = zipfile.ZipInfo(lambda_handler)
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o777 << 16  # give full access
        zf.writestr(info, lambda_code)
    zip_buffer.seek(0)

    # Delete existing function
    with suppress(Exception):
        lambda_client.delete_function(FunctionName="my-function")

    # Create a lambda function that creates a file on our test S3 bucket
    lambda_response = lambda_client.create_function(
        FunctionName="my-function",
        Runtime="python3.9",
        Code={
            "ZipFile": zip_buffer.read(),
        },
        Handler=f"{re.sub('.py', '', lambda_handler)}.lambda_handler",
        Role="arn:aws:iam::123456789012:role/lambda-role",
        Timeout=10,
        MemorySize=128,
    )
    function_arn = lambda_response["FunctionArn"]

    # Wait for the function status to become Active, from Pending before proceeding
    counter = 0
    while True:
        lambda_response = lambda_client.get_function(FunctionName=function_arn)
        if lambda_response["Configuration"]["State"] == "Active":
            break
        counter += 1
        # If we get to 10, then fail the text
        limit = 10

        # Give it more time if running in GitHub Actions
        if github_actions():
            limit = 30
        if counter >= limit:
            raise Exception(
                "Lambda function failed to become active in reasonable time"
            )
        time.sleep(1)

    # Manually call the function to check it's actually working
    if invoke:
        lambda_client.invoke(
            FunctionName=function_arn,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        # Make sure the response is a 200
        assert lambda_response["ResponseMetadata"]["HTTPStatusCode"] == 200

    return function_arn


@pytest.fixture(scope="module")
def setup_bucket(credentials):
    # This all relies on docker container for the AWS stack being set up and running
    # The AWS CLI should also be installed

    buckets = [BUCKET_NAME, BUCKET_NAME_1, BUCKET_NAME_2]
    # Delete existing buckets and recreate
    for bucket in buckets:
        subprocess.run(
            ["awslocal", "s3", "rb", f"s3://{bucket}", "--force"], check=False
        )
        subprocess.run(["awslocal", "s3", "mb", f"s3://{bucket}"], check=False)


def test_remote_handler(credentials):
    execution_obj = execution.Execution(
        None, "call-lambda-function", lambda_execution_task_definition
    )

    execution_obj._set_remote_handlers()

    # Validate some things were set as expected
    assert execution_obj.remote_handlers[0].__class__.__name__ == "LambdaExecution"


def test_run_lambda_function(credentials, lambda_client, s3_client, setup_bucket):
    function_arn = create_lambda_function(
        lambda_client,
        "lambda_write_to_s3.py",
        {
            "bucket_name": BUCKET_NAME,
            "file_name": "function_test.txt",
        },
    )

    # Check that the file has been created in the bucket
    s3_response = s3_client.head_object(
        Bucket=BUCKET_NAME,
        Key="function_test.txt",
    )
    assert s3_response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # Delete the object from the bucket
    s3_client.delete_object(
        Bucket=BUCKET_NAME,
        Key="function_test.txt",
    )

    with pytest.raises(botocore.exceptions.ClientError) as ex:
        s3_client.head_object(
            Bucket=BUCKET_NAME,
            Key="function_test.txt",
        )
    # Check the exception is a 404
    assert ex.value.response["Error"]["Code"] == "404"

    # Update the task definition with the ARN of the function we just created
    lambda_execution_task_definition_copy = lambda_execution_task_definition.copy()
    lambda_execution_task_definition_copy["functionArn"] = function_arn
    lambda_execution_task_definition_copy["payload"]["bucket"] = BUCKET_NAME

    # Call the execution and check whether the lambda function ran successfully
    execution_obj = execution.Execution(
        None, "call-lambda-function", lambda_execution_task_definition_copy
    )

    logging.getLogger("boto3").setLevel(logging.DEBUG)
    logging.getLogger("botocore").setLevel(logging.DEBUG)

    assert execution_obj.run()

    # Wait a second for the lambda to finish
    time.sleep(2)

    # Check that the lambda function has run, and created the file
    s3_response = s3_client.head_object(
        Bucket=BUCKET_NAME,
        Key="function_test.txt",
    )
    assert s3_response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # Invoke it again, but using the RequestResponse invocation type
    # This should return a 200 response

    # clone lambda_execution_task_definition
    lambda_execution_task_definition_copy = lambda_execution_task_definition.copy()
    lambda_execution_task_definition_copy["functionArn"] = function_arn
    lambda_execution_task_definition_copy["payload"]["bucket"] = BUCKET_NAME
    lambda_execution_task_definition_copy["invocationType"] = "RequestResponse"
    execution_obj = execution.Execution(
        None, "call-lambda-function", lambda_execution_task_definition_copy
    )
    assert execution_obj.run()


def test_run_lambda_function_with_failure(credentials, lambda_client):
    function_arn = create_lambda_function(lambda_client, "lambda_failure.py", {})

    # Update the task definition with the ARN of the function we just created
    lambda_execution_task_definition_copy = lambda_execution_task_definition.copy()
    lambda_execution_task_definition_copy["functionArn"] = function_arn
    lambda_execution_task_definition_copy["invocationType"] = "RequestResponse"

    # Call the execution and check whether the lambda function ran successfully
    execution_obj = execution.Execution(
        None, "call-lambda-function-failure", lambda_execution_task_definition_copy
    )

    logging.getLogger("boto3").setLevel(logging.DEBUG)
    logging.getLogger("botocore").setLevel(logging.DEBUG)

    # Wait a second for the lambda to finish
    time.sleep(2)

    assert not execution_obj.run()


def test_run_non_existent_lambda_function(credentials):
    # Update the task definition with the ARN of the function we just created
    lambda_execution_task_definition_copy = lambda_execution_task_definition.copy()
    lambda_execution_task_definition_copy["functionArn"] = "invalid-arn"

    # Call the execution and check whether the lambda function ran successfully
    execution_obj = execution.Execution(
        None,
        "call-non-existent-lambda-function-failure",
        lambda_execution_task_definition_copy,
    )

    logging.getLogger("boto3").setLevel(logging.DEBUG)
    logging.getLogger("botocore").setLevel(logging.DEBUG)

    assert not execution_obj.run()


def test_run_lambda_function_with_invalid_config():
    lambda_execution_task_definition_copy = lambda_execution_task_definition.copy()
    del lambda_execution_task_definition_copy["functionArn"]
    execution_obj = execution.Execution(
        None, "call-lambda-function-invalid", lambda_execution_task_definition_copy
    )
    with pytest.raises(opentaskpy.exceptions.InvalidConfigError):
        execution_obj.run()


def test_run_lambda_function_with_invalid_payload(lambda_client):
    # Create the lambda function
    function_arn = create_lambda_function(
        lambda_client,
        "lambda_write_to_s3.py",
        {
            "bucket_name": BUCKET_NAME_1,
            "file_name": "function_test.txt",
        },
    )

    lambda_execution_task_definition_invalid_payload = (
        lambda_execution_task_definition.copy()
    )
    # Update the task definition with the ARN of the function we just created
    lambda_execution_task_definition_invalid_payload["functionArn"] = function_arn
    # Remove the payload from the definition
    lambda_execution_task_definition_invalid_payload.pop("payload")
    lambda_execution_task_definition_invalid_payload["invocationType"] = (
        "RequestResponse"
    )

    # Call the execution and check whether the lambda function ran successfully
    execution_obj = execution.Execution(
        None, "call-lambda-function", lambda_execution_task_definition_invalid_payload
    )

    assert not execution_obj.run()

    # Try the same, but using an async call to the function
    lambda_execution_task_definition_invalid_payload["invocationType"] = "Event"
    execution_obj = execution.Execution(
        None, "call-lambda-function", lambda_execution_task_definition_invalid_payload
    )

    # This is expected to work, because there's no validation of the payload when invoked this way
    assert execution_obj.run()


def test_lambda_execution_batch_timeout(tmpdir, lambda_client):
    # Create the function, but dont invoke it, as it runs too long
    function_arn = create_lambda_function(
        lambda_client, "lambda_sleep_60.py", None, invoke=False
    )

    # set the arn in lambda_execution_task_definition
    lambda_execution_task_definition["functionArn"] = function_arn
    # Set invocationType to RequestResponse so that it blocks, and will timeout
    lambda_execution_task_definition["invocationType"] = "RequestResponse"

    # We need to write the lambda execution definition to a file for the config_loader to read from
    with open(f"{tmpdir}/lambda_execution_task_definition.json", "w") as f:
        json.dump(lambda_execution_task_definition, f)

    # Write a dummy variables file
    fs.create_files([{f"{tmpdir}/variables.json": {"content": "{}"}}])

    config_loader = ConfigLoader(tmpdir)

    batch_obj = batch.Batch(
        None, "timeout", lambda_batch_task_definition, config_loader
    )

    # Batch is configured to timeout first, though since we cannot kill the actual lambda thread, it'll still block until the lambda function times out
    assert not batch_obj.run()
