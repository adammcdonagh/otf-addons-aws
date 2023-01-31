import datetime
import os
import shutil
import subprocess
import threading
import unittest

from file_helper import BASE_DIRECTORY, create_directory, write_test_file
from opentaskpy.taskhandlers import transfer

from opentaskpy import exceptions


class S3HandlerTest(unittest.TestCase):

    os.environ["OTF_NO_LOG"] = "0"
    os.environ["OTF_LOG_LEVEL"] = "DEBUG"

    BUCKET_NAME = "otf-addons-aws-s3-test"
    BUCKET_NAME_2 = "otf-addons-aws-s3-test-2"

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
                "protocol": {
                    "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"
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
                "hostname": "172.16.0.12",
                "directory": "/tmp/testFiles/dest",
                "protocol": {
                    "name": "ssh",
                    "credentials": {"username": "application"},
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
                "credentials": {"username": "application"},
            },
        },
        "destination": [
            {
                "bucket": BUCKET_NAME,
                "directory": "dest",
                "fileRegex": ".*\\.txt",
                "protocol": {
                    "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"
                },
            }
        ],
    }

    @classmethod
    def setUpClass(cls):
        os.environ["AWS_ACCESS_KEY_ID"] = "test"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
        os.environ["AWS_ENDPOINT_URL"] = "http://localhost:4566"  # For localstack

        # Ensure the test directory exists
        create_directory(f"{BASE_DIRECTORY}/src")

        # This all relies on docker container for the AWS stack being set up and running
        # The AWS CLI should also be installed

        buckets = [cls.BUCKET_NAME, cls.BUCKET_NAME_2]
        # Delete existing buckets and recreate
        for bucket in buckets:
            subprocess.run(["awslocal", "s3", "rb", f"s3://{bucket}", "--force"])
            subprocess.run(["awslocal", "s3", "mb", f"s3://{bucket}"])

    def setUp(self):
        pass

    def test_remote_handler(self):

        transfer_obj = transfer.Transfer(
            "s3-file-watch", self.s3_file_watch_task_definition
        )

        transfer_obj._set_remote_handlers()

        # Validate some things were set as expected
        self.assertEqual(
            transfer_obj.source_remote_handler.__class__.__name__, "S3Transfer"
        )
        # dest_remote_handler should be None
        self.assertIsNone(transfer_obj.dest_remote_handlers)

    def test_s3_file_watch(self):

        transfer_obj = transfer.Transfer(
            "s3-file-watch", self.s3_file_watch_task_definition
        )

        # Create a file to watch for with the current date
        datestamp = datetime.datetime.now().strftime("%Y%m%d")

        # Write a test file locally
        write_test_file(f"{BASE_DIRECTORY}/src/{datestamp}.txt", content="test1234")

        # Write the dummy file to the test S3 bucket

        with self.assertRaises(exceptions.RemoteFileNotFoundError) as cm:
            transfer_obj.run()
        self.assertIn("No files found after ", cm.exception.args[0])

        # This time write the contents after 5 seconds
        t = threading.Timer(
            5,
            self.create_s3_file,
            [f"{BASE_DIRECTORY}/src/{datestamp}.txt", "src/test.txt"],
        )
        t.start()
        print("Started thread - Expect file in 5 seconds, starting task-run now...")

        self.assertTrue(transfer_obj.run())

    def test_s3_to_s3_copy(self):

        transfer_obj = transfer.Transfer("s3-to-s3", self.s3_to_s3_copy_task_definition)

        # Create a file to watch for with the current date
        datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        # Write a test file locally
        write_test_file(f"{BASE_DIRECTORY}/src/{datestamp}.txt", content="test1234")
        self.create_s3_file(f"{BASE_DIRECTORY}/src/{datestamp}.txt", "src/test.txt")

        self.assertTrue(transfer_obj.run())

        # Check that the file is in the destination bucket
        result = subprocess.run(
            [
                "awslocal",
                "s3",
                "ls",
                f"s3://{self.BUCKET_NAME_2}/dest/test.txt",
            ],
            capture_output=True,
        )
        # Asset result.returncode is 0
        self.assertEqual(result.returncode, 0)

    def test_s3_to_ssh_copy(self):

        transfer_obj = transfer.Transfer(
            "s3-to-ssh", self.s3_to_ssh_copy_task_definition
        )

        # Create a file to watch for with the current date
        datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        # Write a test file locally
        write_test_file(f"{BASE_DIRECTORY}/src/{datestamp}.txt", content="test1234")
        self.create_s3_file(f"{BASE_DIRECTORY}/src/{datestamp}.txt", "src/test.txt")

        self.assertTrue(transfer_obj.run())

    def test_ssh_to_s3_copy(self):

        transfer_obj = transfer.Transfer(
            "ssh-to-s3", self.ssh_to_s3_copy_task_definition
        )

        # Create a file to watch for with the current date
        datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        # Write a test file locally
        # Write a test file locally
        write_test_file(
            f"../open-task-framework/test/testFiles/ssh_2/src/{datestamp}.txt",
            content="test1234",
        )

        self.assertTrue(transfer_obj.run())

        # Check that the file is in the destination bucket
        result = subprocess.run(
            [
                "awslocal",
                "s3",
                "ls",
                f"s3://{self.BUCKET_NAME}/dest/{datestamp}.txt",
            ],
            capture_output=True,
        )
        # Asset result.returncode is 0
        self.assertEqual(result.returncode, 0)

    def create_s3_file(self, local_file, object_key):
        subprocess.run(
            [
                "awslocal",
                "s3",
                "cp",
                local_file,
                f"s3://{self.BUCKET_NAME}/{object_key}",
            ]
        )

    @classmethod
    def tearDownClass(cls):
        buckets = [cls.BUCKET_NAME, cls.BUCKET_NAME_2]
        # Delete the test bucket
        for bucket in buckets:
            subprocess.run(["awslocal", "s3", "rb", f"s3://{bucket}", "--force"])

        # Delete any temporary files created
        dir = f"{BASE_DIRECTORY}/src"
        if os.path.exists(dir):
            shutil.rmtree(dir)
