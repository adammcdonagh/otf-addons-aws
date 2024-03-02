# pylint: skip-file
import datetime
import os
import subprocess
import threading
from copy import deepcopy

import botocore
import pytest
from opentaskpy.taskhandlers import transfer
from pytest_shell import fs

from opentaskpy import exceptions
from opentaskpy.addons.aws.remotehandlers.s3 import S3Transfer
from tests.fixtures.localstack import *  # noqa: F403, F405

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
            "assume_role_arn": "arn:aws:iam::01234567890:role/dummy-role",
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
    assert "No files found after " in cm.value.args[0]

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


def create_s3_file(s3_client, local_file, object_key):
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
        Body=open(local_file, "rb"),
    )
