import io
import json
import logging
import os
import subprocess
import zipfile

import botocore.exceptions
import opentaskpy.logging
import pytest
from fixtures.localstack import *  # noqa:F401
from opentaskpy.taskhandlers import execution

os.environ["OTF_NO_LOG"] = "0"
os.environ["OTF_LOG_LEVEL"] = "DEBUG"

BUCKET_NAME = "otf-addons-aws-lambda-execution-test"
logger = opentaskpy.logging.init_logging(__name__)

logger.setLevel(logging.DEBUG)

root_dir_ = get_root_dir()

lambda_execution_task_definition = {
    "type": "execution",
    "functionArn": None,
    "payload": {"bucket_name": BUCKET_NAME, "file_name": "function_test.txt"},
    "protocol": {
        "name": "opentaskpy.addons.aws.remotehandlers.lambda.LambdaExecution",
    },
}


@pytest.fixture(scope="function")
def setup_bucket(credentials):
    # This all relies on docker container for the AWS stack being set up and running
    # The AWS CLI should also be installed

    buckets = [BUCKET_NAME]
    # Delete existing buckets and recreate
    for bucket in buckets:
        subprocess.run(["awslocal", "s3", "rb", f"s3://{bucket}", "--force"])
        subprocess.run(["awslocal", "s3", "mb", f"s3://{bucket}"])


def test_remote_handler():
    execution_obj = execution.Execution(
        "call-lambda-function", lambda_execution_task_definition
    )

    execution_obj._set_remote_handlers()

    # Validate some things were set as expected
    assert execution_obj.remote_handlers[0].__class__.__name__ == "LambdaExecution"


def test_run_lambda_function(setup_bucket, lambda_client, s3_client):
    # Read in lambda_write_to_s3.py
    lambda_code = None
    with open(os.path.join(root_dir_, "../tests", "lambda_write_to_s3.py"), "rb") as f:
        lambda_code = f.read()

    zip_buffer = io.BytesIO()
    # Write a zip file into the buffer
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        info = zipfile.ZipInfo("lambda_write_to_s3.py")
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o777 << 16  # give full access
        zf.writestr(info, lambda_code)
    zip_buffer.seek(0)

    # Delete existing function
    try:
        lambda_client.delete_function(FunctionName="my-function")
    except Exception:
        pass

    # Create a lambda function that create a file on our test S3 bucket
    lambda_response = lambda_client.create_function(
        FunctionName="my-function",
        Runtime="python3.9",
        Code={
            "ZipFile": zip_buffer.read(),
        },
        Handler="lambda_write_to_s3.lambda_handler",
        Role="arn:aws:iam::123456789012:role/lambda-role",
        Timeout=3,
        MemorySize=128,
    )
    function_arn = lambda_response["FunctionArn"]

    # Manually call the function to check it's actually working
    lambda_client.invoke(
        FunctionName=function_arn,
        InvocationType="RequestResponse",
        Payload=json.dumps(
            {
                "bucket_name": "otf-addons-aws-lambda-execution-test",
                "file_name": "function_test.txt",
            }
        ),
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
    lambda_execution_task_definition["functionArn"] = function_arn

    # Call the execution and check whether the lambda function ran successfully
    execution_obj = execution.Execution(
        "call-lambda-function", lambda_execution_task_definition
    )

    logging.getLogger("boto3").setLevel(logging.DEBUG)
    logging.getLogger("botocore").setLevel(logging.DEBUG)

    assert execution_obj.run()

    # Check that the lambda function has run, and created the file
    s3_response = s3_client.head_object(
        Bucket=BUCKET_NAME,
        Key="function_test.txt",
    )
    assert s3_response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_run_lambda_function_with_invalid_payload(
    setup_bucket, s3_client, lambda_client
):
    # Read in lambda_write_to_s3.py
    lambda_code = None
    with open(os.path.join(root_dir_, "../tests", "lambda_write_to_s3.py"), "rb") as f:
        lambda_code = f.read()

    zip_buffer = io.BytesIO()
    # Write a zip file into the buffer
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        info = zipfile.ZipInfo("lambda_write_to_s3.py")
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o777 << 16  # give full access
        zf.writestr(info, lambda_code)
    zip_buffer.seek(0)

    # Delete existing function
    try:
        lambda_client.delete_function(FunctionName="my-function")
    except Exception:
        pass

    # Create a lambda function that creates a file on our test S3 bucket
    lambda_response = lambda_client.create_function(
        FunctionName="my-function",
        Runtime="python3.9",
        Code={
            "ZipFile": zip_buffer.read(),
        },
        Handler="lambda_write_to_s3.lambda_handler",
        Role="arn:aws:iam::123456789012:role/lambda-role",
        Timeout=3,
        MemorySize=128,
    )
    function_arn = lambda_response["FunctionArn"]

    # Manually call the function to check it's actually working
    lambda_client.invoke(
        FunctionName=function_arn,
        InvocationType="RequestResponse",
        Payload=json.dumps(
            {
                "bucket_name": "otf-addons-aws-lambda-execution-test",
                "file_name": "function_test.txt",
            }
        ),
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
    lambda_execution_task_definition["functionArn"] = function_arn
    # Remove the payload from the definition
    lambda_execution_task_definition.pop("payload")

    # Call the execution and check whether the lambda function ran successfully
    execution_obj = execution.Execution(
        "call-lambda-function", lambda_execution_task_definition
    )

    assert not execution_obj.run()
