# pylint: skip-file
# ruff: noqa
# flake8: noqa
import datetime
import logging
import os
import re
import subprocess
import threading
from copy import deepcopy

import botocore
import freezegun
import pytest
from opentaskpy.taskhandlers import transfer
from pytest_shell import fs

from opentaskpy import exceptions
from opentaskpy.addons.aws.remotehandlers.s3 import S3Transfer
from tests.fixtures.localstack import *

os.environ["OTF_NO_LOG"] = "0"
os.environ["OTF_LOG_LEVEL"] = "DEBUG"

BUCKET_NAME = "otf-addons-aws-s3-test"
BUCKET_NAME_2 = "otf-addons-aws-s3-test-2"


root_dir_ = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "test",
)


s3_file_watch_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": ".*\\.txt",
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
        "fileWatch": {
            "timeout": 10,
        },
    },
}

s3_file_watch_pagination_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": ".*-xxxx\\.txt",
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
        "fileWatch": {
            "timeout": 10,
        },
    },
}


s3_age_conditions_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "fileRegex": ".*\\.txt",
        "conditionals": {
            "age": {
                "gt": 10,
                "lt": 18,
            }
        },
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}

s3_file_size_conditions_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "fileRegex": ".*\\.txt",
        "conditionals": {
            "size": {
                "gt": 10,
                "lt": 20,
            }
        },
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}


s3_file_watch_custom_creds_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            "access_key_id": "test",
            "secret_access_key": "test",
            "region_name": "eu-west-1",
        },
        "fileWatch": {
            "timeout": 10,
            "directory": "src",
            "fileRegex": ".*\\.txt",
        },
    },
}


s3_to_s3_copy_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": ".*\\.txt",
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}

s3_to_s3_copy_2_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": "regex-test-5\\.txt",
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}

s3_to_s3_proxy_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": ".*\\.txt",
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "transferType": "proxy",
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}


s3_to_s3_copy_with_fin_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": ".*\\.txt",
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "flags": {
                "fullPath": "dest/my_fin.fin",
            },
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}


s3_to_s3_pca_delete_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": "file-pca-.*\\.txt",
        "postCopyAction": {
            "action": "delete",
        },
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}


s3_to_s3_pca_move_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": "pca-move\\.txt",
        "postCopyAction": {
            "action": "move",
            "destination": "src/archive/",
        },
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}

s3_to_s3_pca_move_new_bucket_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": "pca-move\\.txt",
        "postCopyAction": {
            "action": "move",
            "destination": f"s3://{BUCKET_NAME_2}/PCA/",
        },
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}

s3_to_s3_assume_role_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": "file-assumerole\\.txt",
        "postCopyAction": {
            "action": "delete",
        },
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            "assume_role_arn": "arn:aws:iam::012345678900:role/dummy-role",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}


s3_to_s3_pca_rename_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": "file-pca-rename-(.*)\\.txt",
        "postCopyAction": {
            "action": "rename",
            "destination": "src/archive/",
            "pattern": "file-pca-rename-(.*)\\.txt",
            "sub": "file-pca-rename-\\1-renamed.txt",
        },
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}

s3_to_s3_rename_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": "file-rename-(.*)\\.txt",
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "rename": {"pattern": "abc", "sub": "def"},
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}

s3_to_s3_proxy_rename_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": "file-rename-proxy-(.*)\\.txt",
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "rename": {"pattern": "abc", "sub": "def"},
            "transferType": "proxy",
            "protocol": {
                "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
            },
        },
    ],
}


s3_to_ssh_copy_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "directory": "src",
        "fileRegex": ".*\\.txt",
        "protocol": {"name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"},
    },
    "destination": [
        {
            "hostname": "127.0.0.1",
            "directory": "/tmp/testFiles/dest",
            "protocol": {
                "name": "ssh",
                "port": "3221",
                "credentials": {
                    "username": "application",
                    "keyFile": f"{root_dir_}/testFiles/id_rsa",
                },
            },
        }
    ],
}

ssh_to_s3_copy_task_definition = {
    "type": "transfer",
    "source": {
        "hostname": "127.0.0.1",
        "directory": "/tmp/testFiles/src",
        "fileRegex": ".*\\.txt",
        "protocol": {
            "name": "ssh",
            "port": "3222",
            "credentials": {
                "username": "application",
                "keyFile": f"{root_dir_}/testFiles/id_rsa",
            },
        },
    },
    "destination": [
        {
            "bucket": BUCKET_NAME,
            "directory": "dest",
            "fileRegex": ".*\\.txt",
            "protocol": {"name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"},
        }
    ],
}


@pytest.fixture(scope="function")
def setup_bucket(credentials, s3_client):
    # This all relies on docker container for the AWS stack being set up and running
    # The AWS CLI should also be installed

    buckets = [BUCKET_NAME, BUCKET_NAME_2]
    # Delete existing buckets and recreate
    for bucket in buckets:
        subprocess.run(
            ["awslocal", "s3", "rb", f"s3://{bucket}", "--force"], check=False
        )
        subprocess.run(["awslocal", "s3", "mb", f"s3://{bucket}"], check=False)


def test_remote_handler():
    transfer_obj = transfer.Transfer(
        None, "s3-file-watch", s3_file_watch_task_definition
    )

    transfer_obj._set_remote_handlers()

    # Validate some things were set as expected
    assert transfer_obj.source_remote_handler.__class__.__name__ == "S3Transfer"

    # dest_remote_handler should be None
    assert transfer_obj.dest_remote_handlers is None


def test_s3_file_watch(s3_client, setup_bucket, tmp_path):
    transfer_obj = transfer.Transfer(
        None, "s3-file-watch", s3_file_watch_task_definition
    )

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d")

    # Write a test file locally
    fs.create_files(
        [
            {f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}},
            {f"{tmp_path}/{datestamp}1.csv": {"content": "test1234"}},
            {f"{tmp_path}/{datestamp}2.pdf": {"content": "test1234"}},
            {f"{tmp_path}/{datestamp}3.docx": {"content": "test1234"}},
        ]
    )

    with pytest.raises(exceptions.RemoteFileNotFoundError) as cm:
        transfer_obj.run()
    assert "No valid files found after " in cm.value.args[0]

    # Upload the 3 non-matching files
    for file in os.listdir(tmp_path):
        if not file.endswith(".txt"):
            create_s3_file(s3_client, f"{tmp_path}/{file}", f"src/{file}")

    # This time write the contents after 5 seconds
    t = threading.Timer(
        5,
        create_s3_file,
        [s3_client, f"{tmp_path}/{datestamp}.txt", "src/test.txt"],
    )
    t.start()
    print(  # noqa: T201
        "Started thread - Expect file in 5 seconds, starting task-run now..."
    )

    assert transfer_obj.run()


def test_s3_age_conditions_size(setup_bucket, tmp_path, s3_client):
    transfer_obj = transfer.Transfer(
        None, "s3-age-conditions", s3_age_conditions_task_definition
    )

    # Write a test file locally
    files = [
        {f"{tmp_path}/too_old_file.txt": {"content": "123"}},
        {f"{tmp_path}/correct_file.txt": {"content": "123"}},
        {f"{tmp_path}/too_new_file.txt": {"content": "123"}},
    ]
    fs.create_files(files)
    import time

    for file in files:
        file_name = os.path.basename(list(file.keys())[0])
        create_s3_file(s3_client, list(file.keys())[0], f"src/{file_name}")
        # Sleep 6 seconds
        time.sleep(6)

    assert transfer_obj.run()

    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    # check that the correct_file.txt is in the bucket, and not the other 2
    assert len(objects["Contents"]) == 1
    assert objects["Contents"][0]["Key"] == "dest/correct_file.txt"


def test_s3_file_conditions_size(setup_bucket, tmp_path, s3_client):
    transfer_obj = transfer.Transfer(
        None, "s3-file-size-conditions", s3_file_size_conditions_task_definition
    )

    # Write a test file locally
    files = [
        {f"{tmp_path}/too_small_file.txt": {"content": "1"}},
        {f"{tmp_path}/correct_file.txt": {"content": "test12345678"}},
        {f"{tmp_path}/too_large_file.txt": {"content": "test12345678901234567890"}},
    ]
    fs.create_files(files)
    for file in files:
        file_name = os.path.basename(list(file.keys())[0])
        create_s3_file(s3_client, list(file.keys())[0], f"src/{file_name}")

    assert transfer_obj.run()

    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    # check that the correct_file.txt is in the bucket, and not the other 2
    assert len(objects["Contents"]) == 1
    assert objects["Contents"][0]["Key"] == "dest/correct_file.txt"


def test_s3_to_s3_copy(setup_bucket, s3_client, tmp_path):
    transfer_obj = transfer.Transfer(None, "s3-to-s3", s3_to_s3_copy_task_definition)

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(s3_client, f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    s3_response = s3_client.head_object(
        Bucket=BUCKET_NAME_2,
        Key="dest/test.txt",
    )
    assert s3_response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_s3_to_s3_copy_2(setup_bucket, s3_client, tmp_path):
    transfer_obj = transfer.Transfer(None, "s3-to-s3", s3_to_s3_copy_2_task_definition)

    # Ensure the source bucket is empty first, and the dest too
    s3_client.delete_object(Bucket=BUCKET_NAME, Key="src/*")
    s3_client.delete_object(Bucket=BUCKET_NAME_2, Key="dest/*")

    # Create 10x files, but only one that matches the regex for the transfer
    for i in range(10):
        fs.create_files([{f"{tmp_path}/regex-test-{i}.txt": {"content": "test1234"}}])
        create_s3_file(
            s3_client, f"{tmp_path}/regex-test-{i}.txt", f"src/regex-test-{i}.txt"
        )

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    s3_response = s3_client.head_object(
        Bucket=BUCKET_NAME_2,
        Key="dest/regex-test-5.txt",
    )
    assert s3_response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # Ensure that there are no other files in the dest
    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    assert len(objects["Contents"]) == 1


def test_s3_file_matching(setup_bucket, s3_client, tmp_path):
    # Ensure the source bucket is empty first
    s3_client.delete_object(Bucket=BUCKET_NAME, Key="src/*")

    # Create 10x files, but only one that matches the regex for the transfer exactly.
    # Create a copy of each file in another subdir too, and make sure it doesn't match that path too
    for i in range(10):
        fs.create_files([{f"{tmp_path}/regex-test-{i}.txt": {"content": "test1234"}}])
        create_s3_file(
            s3_client, f"{tmp_path}/regex-test-{i}.txt", f"src/regex-test-{i}.txt"
        )
        create_s3_file(
            s3_client,
            f"{tmp_path}/regex-test-{i}.txt",
            f"src/archive/regex-test-{i}.txt",
        )

    # Create a new S3 remotehandler object
    s3_remote_handler = S3Transfer(s3_to_s3_copy_2_task_definition["source"])

    # Check that only 1 file is matched
    assert (
        len(
            s3_remote_handler.list_files(
                directory=s3_remote_handler.spec["directory"],
                file_pattern=s3_remote_handler.spec["fileRegex"],
            )
        )
        == 1
    )


def test_s3_to_s3_proxy(setup_bucket, s3_client, tmp_path):
    transfer_obj = transfer.Transfer(None, "s3-to-s3", s3_to_s3_proxy_task_definition)

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(s3_client, f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    s3_response = s3_client.head_object(
        Bucket=BUCKET_NAME_2,
        Key="dest/test.txt",
    )
    assert s3_response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_local_to_s3_proxy(setup_bucket, s3_client, tmp_path):

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally
    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])

    # Override the definition to use local source instead
    local_to_s3_task_definition_copy = deepcopy(s3_to_s3_proxy_task_definition)
    local_to_s3_task_definition_copy["source"] = {
        "directory": f"{tmp_path}",
        "fileRegex": f"{datestamp}\\.txt",
        "protocol": {"name": "local"},
    }

    transfer_obj = transfer.Transfer(
        None, "local-to-s3", local_to_s3_task_definition_copy
    )

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    s3_response = s3_client.head_object(
        Bucket=BUCKET_NAME_2,
        Key=f"dest/{datestamp}.txt",
    )
    assert s3_response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_s3_to_s3_invalid_source(setup_bucket, s3_client, tmp_path):
    s3_to_s3_copy_task_definition_copy = deepcopy(s3_to_s3_copy_task_definition)

    s3_to_s3_copy_task_definition_copy["source"]["bucket"] = "invalid-bucket"

    transfer_obj = transfer.Transfer(
        None, "s3-to-s3-invalid-source", s3_to_s3_copy_task_definition_copy
    )

    with pytest.raises(botocore.errorfactory.ClientError):
        transfer_obj.run()


def test_s3_to_s3_invalid_destination(credentials, setup_bucket, s3_client, tmp_path):
    s3_to_s3_copy_task_definition_copy = deepcopy(s3_to_s3_copy_task_definition)

    s3_to_s3_copy_task_definition_copy["destination"][0]["bucket"] = "invalid-bucket"

    # Create a file to find
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(s3_client, f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    transfer_obj = transfer.Transfer(
        None, "s3-to-s3-invalid-destination", s3_to_s3_copy_task_definition_copy
    )

    with pytest.raises(exceptions.RemoteTransferError):
        transfer_obj.run()


def test_s3_to_s3_with_fin_copy(setup_bucket, tmp_path, s3_client):
    transfer_obj = transfer.Transfer(
        None, "s3-to-s3-with-fin", s3_to_s3_copy_with_fin_task_definition
    )

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(s3_client, f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    assert transfer_obj.run()

    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)

    assert len(objects["Contents"]) == 2
    # Check that the file and fin file are in the destination bucket
    object_keys = [obj["Key"] for obj in objects["Contents"]]
    assert "dest/test.txt" in object_keys
    assert "dest/my_fin.fin" in object_keys


def test_s3_to_s3_copy_disable_bucket_owner_acl(setup_bucket, s3_client, tmp_path):
    s3_to_s3_copy_task_definition_copy = deepcopy(s3_to_s3_copy_task_definition)
    s3_to_s3_copy_task_definition_copy["destination"][0]["protocol"][
        "disableBucketOwnerControlACL"
    ] = True
    transfer_obj = transfer.Transfer(
        None, "s3-to-s3", s3_to_s3_copy_task_definition_copy
    )

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(s3_client, f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    assert transfer_obj.run()

    # Can't really test this works with localstack, since there's no actual IAM permissions set on anything
    # Check that the file is in the destination bucket
    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    assert len(objects["Contents"]) == 1
    assert objects["Contents"][0]["Key"] == "dest/test.txt"


def test_local_to_s3_assume_role_expiry_long_token_expiry(
    tmp_path, credentials_aws_dev
):
    # There's no way to refresh a token during a multipart transfer, or to resume after a failure,
    # so user always need to specify a long enough token expiry time for their transfer

    fs.create_files([{f"{tmp_path}/tokentest_long.txt": {"content": "test1234"}}])

    task_definition = {
        "type": "transfer",
        "source": {
            "directory": tmp_path,
            "fileRegex": "tokentest_long.txt",
            "protocol": {"name": "local"},
        },
        "destination": [
            {
                "bucket": os.environ["S3_AWS_BUCKET_NAME"],
                "directory": "dest",
                "protocol": {
                    "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
                    "assume_role_arn": os.environ["S3_AWS_ASSUME_ROLE_ARN"],
                    "token_expiry_seconds": 1234,
                },
            }
        ],
    }

    # Set log levels for boto3 and botocore to DEBUG
    boto3.set_stream_logger(name="boto3", level=logging.DEBUG)
    boto3.set_stream_logger(name="botocore", level=logging.DEBUG)

    transfer_obj = transfer.Transfer(
        None, "local-to-s3-assume-role-long-expiry", task_definition
    )

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    os.environ["OTF_LOG_LEVEL"] = "DEBUG"

    # Create a handler to capture log messages
    log_messages = []

    class LogCaptureHandler(logging.Handler):
        def emit(self, record):
            log_messages.append(record.getMessage())

    # Add the log capture handler to the logger
    logger.addHandler(LogCaptureHandler())

    # Check for a log message like
    # 2025-07-12 14:35:56,185 botocore.endpoint [DEBUG] Making request for OperationModel(name=AssumeRole) with params: {'url_path': '/', 'query_string': '', 'method': 'POST', 'headers': {'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8', 'User-Agent': 'Boto3/1.39.4 md/Botocore#1.39.4 ua/2.1 os/linux#6.6.12-linuxkit md/arch#aarch64 lang/python#3.11.11 md/pyimpl#CPython m/Z,D,b cfg/retry-mode#legacy Botocore/1.39.4'}, 'body': {'Action': 'AssumeRole', 'Version': '2011-06-15', 'RoleArn': 'arn:aws:iam::133141744297:role/assumed-role-otf-addons-aws-dev', 'RoleSessionName': 'OTF1752330956.1843534', 'DurationSeconds': 900}, 'url': 'https://sts.amazonaws.com/', 'context': {'client_region': 'eu-west-1', 'client_config': <botocore.config.Config object at 0xffff8f26fb50>, 'has_streaming_input': False, 'auth_type': 'v4', 'unsigned_payload': None, 'auth_options': ['aws.auth#sigv4'], 'signing': {'region': 'us-east-1', 'signing_name': 'sts'}, 'endpoint_properties': {'authSchemes': [{'name': 'sigv4', 'signingName': 'sts', 'signingRegion': 'us-east-1'}]}}}

    # Run the transfer
    assert transfer_obj.run()
    # Check the log messages for the STS request asking for the session of length 1234 seconds, regex matching against 'DurationSeconds': 1234
    found_duration = False
    for log_message in log_messages:
        if re.search(r"'DurationSeconds': 1234", log_message):
            found_duration = True
    assert found_duration


def test_local_to_s3_assume_role_real(tmp_path, credentials_aws_dev):

    task_definition = {
        "type": "transfer",
        "source": {
            "directory": tmp_path,
            "fileRegex": "tokentest.txt",
            "fileWatch": {"timeout": 9000, "fileRegex": "tokentest\\.txt"},
            "protocol": {"name": "local"},
        },
        "destination": [
            {
                "bucket": os.environ["S3_AWS_BUCKET_NAME"],
                "directory": "dest",
                "protocol": {
                    "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
                    "assume_role_arn": os.environ["S3_AWS_ASSUME_ROLE_ARN"],
                },
            }
        ],
    }

    with freezegun.freeze_time(datetime.datetime.now()) as frozen_datetime:

        transfer_obj = transfer.Transfer(
            None, "local-to-s3-assume-role", task_definition
        )

        # Create a thread to move the time forward by 15 minutes, and start that thread in 2 seconds from now
        def move_time():
            frozen_datetime.move_to(
                datetime.datetime.now() + datetime.timedelta(minutes=15, seconds=5)
            )

            # Create the test file under /tmp_path/tokentest.txt
            fs.create_files([{f"{tmp_path}/tokentest.txt": {"content": "test1234"}}])

        t = threading.Timer(20, move_time)
        t.start()

        # Run the transfer
        # Create a logger
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        os.environ["OTF_LOG_LEVEL"] = "DEBUG"

        # Create a handler to capture log messages
        log_messages = []

        class LogCaptureHandler(logging.Handler):
            def emit(self, record):
                log_messages.append(record.getMessage())

        # Add the log capture handler to the logger
        logger.addHandler(LogCaptureHandler())

        # Run the transfer
        assert transfer_obj.run()

        # Make sure that there's a log line that says "Renewing temporary credentials"
        assert any(
            "Renewing temporary credentials" in log_message
            for log_message in log_messages
        )

        # The log should also contain 2x lines starting with "Assumed role access key id", both should
        # have a different key id though, after the tokens were refreshed. Validate that both lines exist,
        # but that both the key ids are different
        key_ids = ()
        # check the log
        for log_message in log_messages:
            if "Assumed role access key id" in log_message:
                key_id = log_message.split(" ")[-1]
                key_ids += (key_id,)
        assert len(key_ids) == 2
        assert key_ids[0] != key_ids[1]


def test_s3_to_s3_assume_role(setup_bucket, tmp_path, s3_client):
    transfer_obj = transfer.Transfer(
        None, "s3-to-s3-assume_role", s3_to_s3_assume_role_task_definition
    )

    # Create a file to watch for with the current date
    # Write a test file locally

    fs.create_files([{f"{tmp_path}/file-assumerole.txt": {"content": "test1234"}}])
    create_s3_file(
        s3_client, f"{tmp_path}/file-assumerole.txt", "src/file-assumerole.txt"
    )

    assert transfer_obj.run()

    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    # check that the correct_file.txt is in the bucket, and not the other 2
    assert len(objects["Contents"]) == 1
    assert objects["Contents"][0]["Key"] == "dest/file-assumerole.txt"

    # Check that the file is not in the source bucket
    objects = s3_client.list_objects(Bucket=BUCKET_NAME)
    assert "Contents" not in objects


def test_s3_to_s3_copy_pca_delete(setup_bucket, tmp_path, s3_client):
    transfer_obj = transfer.Transfer(
        None, "s3-to-s3-pca-delete", s3_to_s3_pca_delete_task_definition
    )

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/file-pca-{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(
        s3_client,
        f"{tmp_path}/file-pca-{datestamp}.txt",
        f"src/file-pca-{datestamp}.txt",
    )

    assert transfer_obj.run()

    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    # check that the correct_file.txt is in the bucket, and not the other 2
    assert len(objects["Contents"]) == 1
    assert objects["Contents"][0]["Key"] == f"dest/file-pca-{datestamp}.txt"

    # Check that the file is not in the source bucket
    objects = s3_client.list_objects(Bucket=BUCKET_NAME)
    assert "Contents" not in objects


def test_s3_to_s3_copy_pca_move(setup_bucket, tmp_path, s3_client):
    transfer_obj = transfer.Transfer(
        None, "s3-to-s3-pca-move", s3_to_s3_pca_move_task_definition
    )

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/pca-move.txt": {"content": "test1234"}}])
    create_s3_file(s3_client, f"{tmp_path}/pca-move.txt", "src/pca-move.txt")

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    assert len(objects["Contents"]) == 1
    assert objects["Contents"][0]["Key"] == "dest/pca-move.txt"

    # Check that the file has been moved to archive
    objects = s3_client.list_objects(Bucket=BUCKET_NAME)
    assert len(objects["Contents"]) == 1
    assert objects["Contents"][0]["Key"] == "src/archive/pca-move.txt"


def test_s3_to_s3_copy_pca_move_new_bucket(setup_bucket, tmp_path, s3_client):
    transfer_obj = transfer.Transfer(
        None,
        "s3-to-s3-pca-move-new-bucket",
        s3_to_s3_pca_move_new_bucket_task_definition,
    )

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/pca-move.txt": {"content": "test1234"}}])
    create_s3_file(s3_client, f"{tmp_path}/pca-move.txt", "src/pca-move.txt")

    assert transfer_obj.run()

    # Check that the file is in the destination bucket (as well as the moved file)
    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    assert len(objects["Contents"]) == 2
    assert any(obj["Key"] == "dest/pca-move.txt" for obj in objects["Contents"])

    # Check that the file has been moved to the new location in the new bucket
    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    assert len(objects["Contents"]) == 2
    # Check there's a key named "PCA/pca-move.txt"
    assert any(obj["Key"] == "PCA/pca-move.txt" for obj in objects["Contents"])

    # Try again but change the post copy move destination to the root of the bucket instead
    s3_to_s3_pca_move_new_bucket_task_definition_copy = deepcopy(
        s3_to_s3_pca_move_new_bucket_task_definition
    )
    s3_to_s3_pca_move_new_bucket_task_definition_copy["source"]["postCopyAction"][
        "destination"
    ] = f"s3://{BUCKET_NAME_2}/"

    fs.create_files([{f"{tmp_path}/pca-move.txt": {"content": "test1234"}}])
    create_s3_file(s3_client, f"{tmp_path}/pca-move.txt", "src/pca-move.txt")

    transfer_obj = transfer.Transfer(
        None,
        "s3-to-s3-pca-move-new-bucket-2",
        s3_to_s3_pca_move_new_bucket_task_definition_copy,
    )

    assert transfer_obj.run()

    # Check the file exists in the root of the bucket
    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    assert any(obj["Key"] == "pca-move.txt" for obj in objects["Contents"])


def test_s3_to_s3_copy_pca_rename(setup_bucket, tmp_path, s3_client):
    transfer_obj = transfer.Transfer(
        None, "s3-to-s3-pca-rename", s3_to_s3_pca_rename_task_definition
    )

    # Write a test file locally
    fs.create_files([{f"{tmp_path}/file-pca-rename-1234.txt": {"content": "test1234"}}])
    create_s3_file(
        s3_client,
        f"{tmp_path}/file-pca-rename-1234.txt",
        "src/file-pca-rename-1234.txt",
    )

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    assert len(objects["Contents"]) == 1
    assert objects["Contents"][0]["Key"] == "dest/file-pca-rename-1234.txt"

    objects = s3_client.list_objects(Bucket=BUCKET_NAME)
    assert len(objects["Contents"]) == 1
    assert (
        objects["Contents"][0]["Key"] == "src/archive/file-pca-rename-1234-renamed.txt"
    )


def test_s3_to_s3_copy_rename(setup_bucket, tmp_path, s3_client):
    transfer_obj = transfer.Transfer(
        None, "s3-to-s3-rename", s3_to_s3_rename_task_definition
    )

    # Write a test file locally
    fs.create_files([{f"{tmp_path}/file-rename-abc.txt": {"content": "test1234"}}])
    create_s3_file(
        s3_client,
        f"{tmp_path}/file-rename-abc.txt",
        "src/file-rename-abc.txt",
    )

    assert transfer_obj.run()

    # Check that the file is in the destination bucket with new name
    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    assert len(objects["Contents"]) == 1
    assert objects["Contents"][0]["Key"] == "dest/file-rename-def.txt"


def test_s3_to_s3_proxy_rename(setup_bucket, s3_client, tmp_path):
    transfer_obj = transfer.Transfer(
        None, "s3-to-s3-proxy-rename", s3_to_s3_proxy_rename_task_definition
    )

    # Write a test file locally
    fs.create_files(
        [{f"{tmp_path}/file-rename-proxy-abc.txt": {"content": "test1234"}}]
    )
    create_s3_file(
        s3_client,
        f"{tmp_path}/file-rename-proxy-abc.txt",
        "src/file-rename-proxy-abc.txt",
    )

    assert transfer_obj.run()

    # Check that the file is in the destination bucket with new name
    objects = s3_client.list_objects(Bucket=BUCKET_NAME_2)
    assert len(objects["Contents"]) == 1
    assert objects["Contents"][0]["Key"] == "dest/file-rename-proxy-def.txt"


def test_s3_file_watch_custom_creds(
    setup_bucket,
    tmp_path,
    s3_client,
):
    transfer_obj = transfer.Transfer(
        None, "s3-file-watch-custom-creds", s3_file_watch_custom_creds_task_definition
    )

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d")

    # Write a test file locally
    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    # Write the dummy file to the test S3 bucket
    create_s3_file(s3_client, f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    assert transfer_obj.run()


def test_s3_file_watch_pagination(s3_client, setup_bucket, tmp_path):
    transfer_obj = transfer.Transfer(
        None, "s3-file-watch-pagination", s3_file_watch_pagination_task_definition
    )

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d")

    # Write 1010 files locally
    for i in range(1010):
        fs.create_files([{f"{tmp_path}/{datestamp}-{i}.txt": {"content": "test1234"}}])
        create_s3_file(
            s3_client, f"{tmp_path}/{datestamp}-{i}.txt", f"src/{datestamp}-{i}.txt"
        )

    # Now write another
    fs.create_files([{f"{tmp_path}/{datestamp}-xxxx.txt": {"content": "test1234"}}])
    create_s3_file(
        s3_client, f"{tmp_path}/{datestamp}-xxxx.txt", f"src/{datestamp}-xxxx.txt"
    )

    # File should be found
    assert transfer_obj.run()


def create_s3_file(s3_client, local_file, object_key):
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
        Body=open(local_file, "rb"),
    )


@pytest.fixture(scope="function")
def credentials_aws_dev(cleanup_credentials):

    if not os.environ.get("GITHUB_ACTIONS"):
        # Look for a .env file in the root of the project
        env_file = os.path.join(root_dir_, "../.env")
        if os.path.isfile(env_file):
            with open(env_file) as f:
                for line in f:
                    if line.startswith("#"):
                        continue
                    key, value = line.strip().split("=")
                    os.environ[key] = value

            # Set the environment variables, but remove S3_ from them
            os.environ["AWS_ACCESS_KEY_ID"] = os.environ["S3_AWS_ACCESS_KEY_ID"]
            os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ["S3_AWS_SECRET_ACCESS_KEY"]
            os.environ["AWS_DEFAULT_REGION"] = os.environ["S3_AWS_DEFAULT_REGION"]

            # If ASSUME_ROLE_ARN is

    if os.environ.get("GITHUB_ACTIONS"):
        if not os.environ.get("S3_AWS_ACCESS_KEY_ID"):
            print("ERROR: Missing AWS creds")  # noqa: T201
            assert False

        # Read the AWS credentials from the environment
        os.environ["AWS_ACCESS_KEY_ID"] = os.environ["S3_AWS_ACCESS_KEY_ID"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ["S3_AWS_SECRET_ACCESS_KEY"]
        os.environ["AWS_DEFAULT_REGION"] = os.environ["S3_AWS_DEFAULT_REGION"]
        os.environ["ASSUME_ROLE_ARN"] = os.environ["S3_AWS_ASSUME_ROLE_ARN"]
        if os.environ.get("AWS_ENDPOINT_URL"):
            del os.environ["AWS_ENDPOINT_URL"]
