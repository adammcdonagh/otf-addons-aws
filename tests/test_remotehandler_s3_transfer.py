import datetime
import os
import shutil
import subprocess
import threading

import pytest
from fixtures.localstack import *  # noqa:F401
from opentaskpy.taskhandlers import transfer
from pytest_shell import fs

from opentaskpy import exceptions

os.environ["OTF_NO_LOG"] = "0"
os.environ["OTF_LOG_LEVEL"] = "DEBUG"

BUCKET_NAME = "otf-addons-aws-s3-test"
BUCKET_NAME_2 = "otf-addons-aws-s3-test-2"


root_dir_ = get_root_dir()

s3_file_watch_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "protocol": {
            "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        },
        "fileWatch": {
            "timeout": 10,
            "directory": "src",
            "fileRegex": ".*\\.txt",
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
        "fileRegex": "file-pca\\.txt",
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
        "fileRegex": "file-pca\\.txt",
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
            "hostname": "172.16.1.12",
            "directory": "/tmp/testFiles/dest",
            "protocol": {
                "name": "ssh",
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
        "hostname": "172.16.1.12",
        "directory": "/tmp/testFiles/src",
        "fileRegex": ".*\\.txt",
        "protocol": {
            "name": "ssh",
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


@pytest.fixture(scope="session")
def test_files(root_dir):
    # Get the root directory of the project

    structure = [
        f"{root_dir}/testFiles/ssh_1/ssh",
        f"{root_dir}/testFiles/ssh_1/src",
        f"{root_dir}/testFiles/ssh_1/dest",
        f"{root_dir}/testFiles/ssh_1/archive",
        f"{root_dir}/testFiles/ssh_2/ssh",
        f"{root_dir}/testFiles/ssh_2/src",
        f"{root_dir}/testFiles/ssh_2/dest",
        f"{root_dir}/testFiles/ssh_2/archive",
    ]
    fs.create_files(structure)


@pytest.fixture(scope="session")
def ssh_1(docker_services):
    docker_services.start("ssh_1")
    port = docker_services.port_for("ssh_1", 22)
    address = f"{docker_services.docker_ip}:{port}"
    return address


@pytest.fixture(scope="session")
def ssh_2(docker_services):
    docker_services.start("ssh_2")
    port = docker_services.port_for("ssh_2", 22)
    address = f"{docker_services.docker_ip}:{port}"
    return address


@pytest.fixture(scope="session")
def setup_ssh_keys(docker_services, root_dir, test_files, ssh_1, ssh_2):
    # Run command locally
    # if ssh key dosent exist yet
    ssh_private_key_file = f"{root_dir}/testFiles/id_rsa"
    if not os.path.isfile(ssh_private_key_file):
        subprocess.run(
            ["ssh-keygen", "-t", "rsa", "-N", "", "-f", ssh_private_key_file]
        ).returncode

        # Copy the file into the ssh directory for each host
        for i in ["1", "2"]:
            shutil.copy(
                ssh_private_key_file, f"{root_dir}/testFiles/ssh_{i}/ssh/id_rsa"
            )
            shutil.copy(
                f"{root_dir}/testFiles/id_rsa.pub",
                f"{root_dir}/testFiles/ssh_{i}/ssh/authorized_keys",
            )

    # Run the docker exec command to create the user
    # Get the current uid for the running process
    uid = str(os.getuid())
    # commands to run
    commands = [
        ("usermod", "-G", "operator", "-a", "application", "-u", uid),
        ("mkdir", "-p", "/home/application/.ssh"),
        ("cp", "/tmp/testFiles/ssh/id_rsa", "/home/application/.ssh"),
        (
            "cp",
            "/tmp/testFiles/ssh/authorized_keys",
            "/home/application/.ssh/authorized_keys",
        ),
        ("chown", "-R", "application", "/home/application/.ssh"),
        ("chmod", "-R", "700", "/home/application/.ssh"),
        ("chown", "-R", "application", "/tmp/testFiles"),
    ]
    for host in ["ssh_1", "ssh_2"]:
        for command in commands:
            docker_services.execute(host, *command)


@pytest.fixture(scope="session")
def setup_bucket(credentials):
    # This all relies on docker container for the AWS stack being set up and running
    # The AWS CLI should also be installed

    buckets = [BUCKET_NAME, BUCKET_NAME_2]
    # Delete existing buckets and recreate
    for bucket in buckets:
        subprocess.run(["awslocal", "s3", "rb", f"s3://{bucket}", "--force"])
        subprocess.run(["awslocal", "s3", "mb", f"s3://{bucket}"])


def test_remote_handler():
    transfer_obj = transfer.Transfer("s3-file-watch", s3_file_watch_task_definition)

    transfer_obj._set_remote_handlers()

    # Validate some things were set as expected
    assert transfer_obj.source_remote_handler.__class__.__name__ == "S3Transfer"

    # dest_remote_handler should be None
    assert transfer_obj.dest_remote_handlers is None


def test_s3_file_watch(setup_bucket, tmp_path):
    transfer_obj = transfer.Transfer("s3-file-watch", s3_file_watch_task_definition)

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d")

    # Write a test file locally
    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])

    # Write the dummy file to the test S3 bucket

    with pytest.raises(exceptions.RemoteFileNotFoundError) as cm:
        transfer_obj.run()
    assert "No files found after " in cm.value.args[0]

    # This time write the contents after 5 seconds
    t = threading.Timer(
        5,
        create_s3_file,
        [f"{tmp_path}/{datestamp}.txt", "src/test.txt"],
    )
    t.start()
    print("Started thread - Expect file in 5 seconds, starting task-run now...")

    assert transfer_obj.run()


def test_s3_age_conditions_size(setup_bucket, tmp_path):
    transfer_obj = transfer.Transfer(
        "s3-age-conditions", s3_age_conditions_task_definition
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
        create_s3_file(list(file.keys())[0], f"src/{file_name}")
        # Sleep 6 seconds
        time.sleep(6)

    assert transfer_obj.run()

    # Check that only correct_file.txt is in the bucket
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME_2}/dest/",
        ],
        stdout=subprocess.PIPE,
    )
    assert "correct_file.txt" in result.stdout.decode("utf-8")
    assert "too_old_file.txt" not in result.stdout.decode("utf-8")
    assert "too_new_file.txt" not in result.stdout.decode("utf-8")


def test_s3_file_conditions_size(setup_bucket, tmp_path):
    transfer_obj = transfer.Transfer(
        "s3-file-size-conditions", s3_file_size_conditions_task_definition
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
        create_s3_file(list(file.keys())[0], f"src/{file_name}")

    assert transfer_obj.run()

    # Check that only correct_file.txt is in the bucket
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME_2}/dest/",
        ],
        stdout=subprocess.PIPE,
    )
    assert "correct_file.txt" in result.stdout.decode("utf-8")
    assert "too_small_file.txt" not in result.stdout.decode("utf-8")
    assert "too_large_file.txt" not in result.stdout.decode("utf-8")


def test_s3_to_s3_copy(setup_bucket, tmp_path):
    transfer_obj = transfer.Transfer("s3-to-s3", s3_to_s3_copy_task_definition)

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME_2}/dest/test.txt",
        ],
        capture_output=True,
    )
    # Asset result.returncode is 0
    assert result.returncode == 0


def test_s3_to_s3_with_fin_copy(setup_bucket, tmp_path):
    transfer_obj = transfer.Transfer(
        "s3-to-s3-with-fin", s3_to_s3_copy_with_fin_task_definition
    )

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME_2}/dest/test.txt",
        ],
        capture_output=True,
    )
    # Asset result.returncode is 0
    assert result.returncode == 0

    # Make sure that the fin file has been created
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME_2}/dest/my_fin.fin",
        ],
        capture_output=True,
    )
    # Asset result.returncode is 0
    assert result.returncode == 0


def test_s3_to_s3_copy_disable_bucket_owner_acl(setup_bucket, tmp_path):
    import copy

    s3_to_s3_copy_task_definition_copy = copy.deepcopy(s3_to_s3_copy_task_definition)
    s3_to_s3_copy_task_definition_copy["destination"][0]["protocol"][
        "disableBucketOwnerControlACL"
    ] = True
    transfer_obj = transfer.Transfer("s3-to-s3", s3_to_s3_copy_task_definition_copy)

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    assert transfer_obj.run()

    # Cant really test this works with localstack, since there's no actual IAM permissions set on anything
    # Check that the file is in the destination bucket
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME_2}/dest/test.txt",
        ],
        capture_output=True,
    )
    # Asset result.returncode is 0
    assert result.returncode == 0


def test_s3_to_s3_copy_pca_delete(setup_bucket, tmp_path):
    transfer_obj = transfer.Transfer(
        "s3-to-s3-pca-delete", s3_to_s3_pca_delete_task_definition
    )

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(f"{tmp_path}/{datestamp}.txt", f"src/{datestamp}.txt")

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME_2}/dest/{datestamp}.txt",
        ],
        capture_output=True,
    )
    # Asset result.returncode is 0
    assert result.returncode == 0

    # Check that the file is not in the source bucket
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME}/src/{datestamp}.txt",
        ],
        capture_output=True,
    )
    # Asset result.returncode is 1
    assert result.returncode == 1


def test_s3_to_s3_copy_pca_move(setup_bucket, tmp_path):
    transfer_obj = transfer.Transfer(
        "s3-to-s3-pca-move", s3_to_s3_pca_move_task_definition
    )

    # Write a test file locally

    fs.create_files([{f"{tmp_path}/pca-move.txt": {"content": "test1234"}}])
    create_s3_file(f"{tmp_path}/pca-move.txt", "src/pca-move.txt")

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME_2}/dest/pca-move.txt",
        ],
        capture_output=True,
    )
    # Asset result.returncode is 0
    assert result.returncode == 0

    # Check that the file has been moved to archive
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME}/src/archive/pca-move.txt",
        ],
        capture_output=True,
    )
    # Asset result.returncode is 1
    assert result.returncode == 1


def test_s3_to_s3_copy_pca_rename(setup_bucket, tmp_path):
    transfer_obj = transfer.Transfer(
        "s3-to-s3-pca-rename", s3_to_s3_pca_rename_task_definition
    )

    # Write a test file locally
    fs.create_files([{f"{tmp_path}/file-pca-rename-1234.txt": {"content": "test1234"}}])
    create_s3_file(
        f"{tmp_path}/file-pca-rename-1234.txt", "src/file-pca-rename-1234.txt"
    )

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME_2}/dest/file-pca-rename-1234.txt",
        ],
        capture_output=True,
    )
    # Asset result.returncode is 0
    assert result.returncode == 0

    # Check that the file has been moved to archive
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME}/src/archive/file-pca-rename-renamed.txt",
        ],
        capture_output=True,
    )
    # Asset result.returncode is 1
    assert result.returncode == 1


def test_s3_to_ssh_copy(setup_bucket, tmp_path, setup_ssh_keys):
    transfer_obj = transfer.Transfer("s3-to-ssh", s3_to_ssh_copy_task_definition)

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally
    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    assert transfer_obj.run()


def test_ssh_to_s3_copy(setup_bucket, root_dir, setup_ssh_keys):
    transfer_obj = transfer.Transfer("ssh-to-s3", ssh_to_s3_copy_task_definition)

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally
    fs.create_files(
        [{f"{root_dir}/testFiles/ssh_2/src/{datestamp}.txt": {"content": "test1234"}}]
    )

    assert transfer_obj.run()

    # Check that the file is in the destination bucket
    result = subprocess.run(
        [
            "awslocal",
            "s3",
            "ls",
            f"s3://{BUCKET_NAME}/dest/{datestamp}.txt",
        ],
        capture_output=True,
    )
    # Asset result.returncode is 0
    assert result.returncode == 0


def test_s3_file_watch_custom_creds(setup_bucket, tmp_path, cleanup_credentials):
    transfer_obj = transfer.Transfer(
        "s3-file-watch-custom-creds", s3_file_watch_custom_creds_task_definition
    )

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d")

    # Write a test file locally
    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    # Write the dummy file to the test S3 bucket
    create_s3_file(f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    assert transfer_obj.run()


def create_s3_file(local_file, object_key):
    subprocess.run(
        [
            "awslocal",
            "s3",
            "cp",
            local_file,
            f"s3://{BUCKET_NAME}/{object_key}",
        ]
    )
