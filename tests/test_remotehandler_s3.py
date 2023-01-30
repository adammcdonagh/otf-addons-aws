import datetime
import os
import subprocess
import threading
import unittest

from file_helper import BASE_DIRECTORY, create_directory, write_test_file
from opentaskpy.taskhandlers import transfer

from opentaskpy import exceptions


class S3HandlerTest(unittest.TestCase):

    BUCKET_NAME = "otf-addons-aws-s3-test"

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

        # Delete existing buckets and recreate
        subprocess.run(["awslocal", "s3", "rb", f"s3://{cls.BUCKET_NAME}", "--force"])
        subprocess.run(["awslocal", "s3", "mb", f"s3://{cls.BUCKET_NAME}"])

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
            [f"{BASE_DIRECTORY}/src/{datestamp}.txt", "src/test.txt", "test1234"],
        )
        t.start()
        print("Started thread - Expect file in 5 seconds, starting task-run now...")

        self.assertTrue(transfer_obj.run())

    def create_s3_file(self, local_file, object_key, content):
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

        # Delete the test bucket
        subprocess.run(["awslocal", "s3", "rb", f"s3://{cls.BUCKET_NAME}", "--force"])
