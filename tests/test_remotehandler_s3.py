import datetime
import os
import shutil
import subprocess
import threading

import pytest
from opentaskpy.taskhandlers import transfer
from pytest_shell import fs

from opentaskpy import exceptions

os.environ["OTF_NO_LOG"] = "0"
os.environ["OTF_LOG_LEVEL"] = "DEBUG"

BUCKET_NAME = "otf-addons-aws-s3-test"
BUCKET_NAME_2 = "otf-addons-aws-s3-test-2"


@pytest.fixture(scope="session")
def root_dir():
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test"
    )


s3_file_watch_task_definition = {
    "type": "transfer",
    "source": {
        "bucket": BUCKET_NAME,
        "protocol": {"name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"},
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
        "protocol": {"name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"},
    },
    "destination": [
        {
            "bucket": BUCKET_NAME_2,
            "directory": "dest",
            "protocol": {"name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"},
        },
    ],
}

root_dir_ = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test"
)

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
            "hostname": "172.16.0.12",
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
        "hostname": "172.16.0.12",
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
def docker_compose_files(root_dir):
    """Get the docker-compose.yml absolute path."""
    return [
        f"{root_dir}/docker-compose.yml",
    ]


@pytest.fixture(scope="session")
def localstack(docker_services):
    docker_services.start("localstack")
    public_port = docker_services.wait_for_service("localstack", 4566)
    url = f"http://{docker_services.docker_ip}:{public_port}"
    return url


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
def setup_bucket(localstack, setup_ssh_keys):
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
    os.environ["AWS_ENDPOINT_URL"] = localstack

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


def test_s3_to_ssh_copy(setup_bucket, tmp_path):
    transfer_obj = transfer.Transfer("s3-to-ssh", s3_to_ssh_copy_task_definition)

    # Create a file to watch for with the current date
    datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Write a test file locally
    fs.create_files([{f"{tmp_path}/{datestamp}.txt": {"content": "test1234"}}])
    create_s3_file(f"{tmp_path}/{datestamp}.txt", "src/test.txt")

    assert transfer_obj.run()


def test_ssh_to_s3_copy(setup_bucket, root_dir):
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
