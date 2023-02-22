import os
import subprocess

import pytest
from fixtures.localstack import *  # noqa:F401
from opentaskpy.taskhandlers import execution

os.environ["OTF_NO_LOG"] = "0"
os.environ["OTF_LOG_LEVEL"] = "DEBUG"

BUCKET_NAME = "otf-addons-aws-s3-execution-test"

root_dir_ = get_root_dir()

s3_execution_task_definition = {
    "type": "execution",
    "bucket": BUCKET_NAME,
    "key": "test_flag.txt",
    "protocol": {
        "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Execution",
    },
}


@pytest.fixture(scope="session")
def setup_bucket(credentials):
    # This all relies on docker container for the AWS stack being set up and running
    # The AWS CLI should also be installed

    buckets = [BUCKET_NAME]
    # Delete existing buckets and recreate
    for bucket in buckets:
        subprocess.run(["awslocal", "s3", "rb", f"s3://{bucket}", "--force"])
        subprocess.run(["awslocal", "s3", "mb", f"s3://{bucket}"])


def test_remote_handler():
    execution_obj = execution.Execution("s3-flag-file", s3_execution_task_definition)

    execution_obj._set_remote_handlers()

    # Validate some things were set as expected
    assert execution_obj.remote_handlers[0].__class__.__name__ == "S3Execution"


def test_s3_touch_file(setup_bucket):
    execution_obj = execution.Execution("s3-flag-file", s3_execution_task_definition)

    assert execution_obj.run()

    # Check that the file has been created in the bucket
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME}/test_flag.txt",
        ],
        stdout=subprocess.PIPE,
    )
    assert "test_flag.txt" in result.stdout.decode("utf-8")
