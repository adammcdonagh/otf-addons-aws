import glob
import os
import re

import boto3
import opentaskpy.logging
from opentaskpy.remotehandlers.remotehandler import RemoteTransferHandler

MAX_OBJECTS_PER_QUERY = 100


class S3Transfer(RemoteTransferHandler):
    TASK_TYPE = "T"

    def __init__(self, spec):
        self.spec = spec

        self.logger = opentaskpy.logging.init_logging(
            __name__, os.environ.get("OTF_TASK_ID"), self.TASK_TYPE
        )

        self.aws_access_key_id = (
            self.spec["access_key_id"]
            if "access_key_id" in self.spec
            else os.environ.get("AWS_ACCESS_KEY_ID")
        )
        self.aws_secret_access_key = (
            self.spec["secret_access_key"]
            if "secret_access_key" in self.spec
            else os.environ.get("AWS_SECRET_ACCESS_KEY")
        )

        # Use boto3 to setup the required object for the transfer
        kwargs = {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
        }
        # If there's an override for endpoint_url in the environment, then use that
        if os.environ.get("AWS_ENDPOINT_URL"):
            kwargs["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL")

        self.s3_client = boto3.client("s3", **kwargs)

    def handle_post_copy_action(self, files):
        raise NotImplementedError()

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
        for file in files:
            # Strip the directory from the file
            file_name = file.split("/")[-1]
            self.logger.debug(f"Transferring file: {file}")
            try:
                self.s3_client.upload_file(
                    file,
                    self.spec["bucket"],
                    f"{self.spec['directory']}/{file_name}",
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

        if isinstance(dest_remote_handler, S3Transfer):
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

        # Otherwise it's a bit more complicated

    def tidy(self):
        self.s3_client.close()
