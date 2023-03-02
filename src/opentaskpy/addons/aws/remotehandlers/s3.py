import glob
import os
import re

import boto3
import opentaskpy.logging
from opentaskpy.remotehandlers.remotehandler import (
    RemoteExecutionHandler,
    RemoteTransferHandler,
)

from .aws import set_aws_creds

MAX_OBJECTS_PER_QUERY = 100


class S3Transfer(RemoteTransferHandler):
    TASK_TYPE = "T"

    def __init__(self, spec):
        self.spec = spec

        self.logger = opentaskpy.logging.init_logging(
            __name__, os.environ.get("OTF_TASK_ID"), self.TASK_TYPE
        )

        set_aws_creds(self)

        # Use boto3 to setup the required object for the transfer
        kwargs = {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "region_name": self.region_name,
        }
        # If there's an override for endpoint_url in the environment, then use that
        if os.environ.get("AWS_ENDPOINT_URL"):
            kwargs["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL")

        self.s3_client = boto3.client("s3", **kwargs)

    def handle_post_copy_action(self, files):
        # Determine the action to take
        # Delete the files
        if self.spec["postCopyAction"]["action"] == "delete":
            self.s3_client.delete_objects(
                Bucket=self.spec["bucket"],
                Delete={
                    "Objects": [{"Key": file} for file in files],
                    "Quiet": True,
                },
            )
        # Copy the files to the new location, and then delete the originals
        if (
            self.spec["postCopyAction"]["action"] == "move"
            or self.spec["postCopyAction"]["action"] == "rename"
        ):
            for file in files:
                # If this is a rename, then we need to determine the new key
                new_file = file
                if self.spec["postCopyAction"]["action"] == "rename":
                    new_file = f"{self.spec['postCopyAction']['destination']}{file.split('/')[-1]}"

                    # Use the pattern and sub values to rename the file correctly
                    new_file = re.sub(
                        self.spec["postCopyAction"]["pattern"],
                        self.spec["postCopyAction"]["sub"],
                        new_file,
                    )

                self.s3_client.copy_object(
                    Bucket=self.spec["bucket"],
                    CopySource={
                        "Bucket": self.spec["bucket"],
                        "Key": file,
                    },
                    Key=f"{self.spec['postCopyAction']['destination']}/{new_file}",
                )
                self.s3_client.delete_objects(
                    Bucket=self.spec["bucket"],
                    Delete={
                        "Objects": [{"Key": file}],
                        "Quiet": True,
                    },
                )
        return 0

    def list_files(self, directory=None, file_pattern=None):
        kwargs = {
            "Bucket": self.spec["bucket"],
            "MaxKeys": MAX_OBJECTS_PER_QUERY,
        }
        if directory:
            kwargs["Prefix"] = directory

        remote_files = dict()

        while True:
            response = self.s3_client.list_objects_v2(**kwargs)

            if response["KeyCount"]:
                for object in response["Contents"]:
                    key = object["Key"]
                    # Get the filename from the key
                    filename = key.split("/")[-1]  #
                    self.logger.debug(f"Found file: {filename}")
                    if file_pattern and not re.match(file_pattern, filename):
                        continue
                    else:
                        # Get the size and modified time
                        file_attr = self.s3_client.head_object(
                            Bucket=self.spec["bucket"], Key=key
                        )

                        remote_files[key] = {
                            "size": file_attr["ContentLength"],
                            "modified_time": file_attr["LastModified"].timestamp(),
                        }

                # This handles the pagination
                # if the NextContinuationToken doesnt exist, then we'll break out
                # of the loop
                try:
                    kwargs["ContinuationToken"] = response["NextContinuationToken"]
                except KeyError:
                    break
            else:
                return None

        return remote_files

    def move_files_to_final_location(self, files):
        raise NotImplementedError()

    # When S3 is the destination
    def pull_files(self, files, remote_spec):
        raise NotImplementedError()

    def push_files_from_worker(self, local_staging_directory):
        result = 0
        files = glob.glob(f"{local_staging_directory}/*")
        kwargs = {}
        if self.bucket_owner_full_control:
            kwargs["ACL"] = "bucket-owner-full-control"

        for file in files:
            # Strip the directory from the file
            file_name = file.split("/")[-1]
            self.logger.debug(
                f"Transferring file: {file} to s3://{self.spec['bucket']}/{file_name}"
            )
            try:
                self.s3_client.upload_file(
                    file,
                    self.spec["bucket"],
                    f"{self.spec['directory']}/{file_name}",
                    ExtraArgs=kwargs,
                )
            except Exception as e:
                self.logger.error(f"Failed to transfer file: {file}")
                self.logger.error(e)
                result = 1

        return result

    def pull_files_to_worker(self, files, local_staging_directory):
        result = 0
        for file in files:
            # Strip the directory from the file
            file_name = file.split("/")[-1]
            self.logger.debug(f"Transferring file: {file}")
            try:
                self.s3_client.download_file(
                    self.spec["bucket"],
                    file,
                    f"{local_staging_directory}/{file_name}",
                )
            except Exception as e:
                self.logger.error(f"Failed to transfer file: {file}")
                self.logger.error(e)
                result = 1

        return result

    def transfer_files(self, files, remote_spec, dest_remote_handler=None):
        # Check the remote handler, if it's another S3Transfer, then it's simple
        # to do an S3 copy via boto

        result = 0
        for file in files:
            # Strip the directory from the file
            file_name = file.split("/")[-1]
            self.logger.debug(f"Transferring file: {file}")
            try:
                self.s3_client.copy(
                    {
                        "Bucket": self.spec["bucket"],
                        "Key": file,
                    },
                    dest_remote_handler.spec["bucket"],
                    f"{dest_remote_handler.spec['directory']}/{file_name}",
                )
            except Exception as e:
                self.logger.error(f"Error transferring file: {file}")
                self.logger.error(e)
                result = 1

        return result

    def create_flag_files(self):
        result = 0

        object_key = self.spec["flags"]["fullPath"]
        self.logger.debug(f"Creating flag file: {object_key}")
        kwargs = {
            "Bucket": self.spec["bucket"],
            "Key": object_key,
        }
        if self.bucket_owner_full_control:
            kwargs["ACL"] = "bucket-owner-full-control"

        try:
            self.s3_client.put_object(**kwargs)
        except Exception as e:
            self.logger.error(f"Error creating flag file: {object_key}")
            self.logger.error(e)
            result = 1

        return result

    def tidy(self):
        self.s3_client.close()


class S3Execution(RemoteExecutionHandler):
    TASK_TYPE = "E"

    def tidy(self):
        self.s3_client.close()

    def __init__(self, spec):
        self.spec = spec

        self.logger = opentaskpy.logging.init_logging(
            __name__, os.environ.get("OTF_TASK_ID"), self.TASK_TYPE
        )

        set_aws_creds(self)

        # Use boto3 to setup the required object
        kwargs = {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "region_name": self.region_name,
        }
        # If there's an override for endpoint_url in the environment, then use that
        if os.environ.get("AWS_ENDPOINT_URL"):
            kwargs["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL")

        self.s3_client = boto3.client("s3", **kwargs)

    # This cannot be long running, so kill doesnt really need to do anything
    def kill(self):
        pass

    def execute(self):
        result = True

        object_key = self.spec["key"]
        bucket = self.spec["bucket"]

        kwargs = {
            "Bucket": bucket,
            "Key": object_key,
        }
        if self.bucket_owner_full_control:
            kwargs["ACL"] = "bucket-owner-full-control"

        # Use the boto client to write a blank file with this name to the bucket
        try:
            self.s3_client.put_object(**kwargs)
        except Exception as e:
            self.logger.error(f"Failed to create flag file: {object_key}")
            self.logger.error(e)
            result = False

        return result
