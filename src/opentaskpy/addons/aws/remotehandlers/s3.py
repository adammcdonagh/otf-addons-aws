"""AWS S3 remote handler."""

import glob
import os
import re
from datetime import datetime
from time import time

import boto3
import opentaskpy.otflogging
from botocore.exceptions import ClientError
from dateutil.tz import tzlocal
from opentaskpy.remotehandlers.remotehandler import (
    RemoteExecutionHandler,
    RemoteTransferHandler,
)

from .creds import set_aws_creds

MAX_OBJECTS_PER_QUERY = 100


class S3Transfer(RemoteTransferHandler):
    """S3 remote transfer handler."""

    TASK_TYPE = "T"

    def __init__(self, spec: dict):
        """Initialise the S3Transfer handler.

        Args:
            spec (dict): The spec for the transfer. This is either the source, or the
            destination spec.
        """
        self.logger = opentaskpy.otflogging.init_logging(
            __name__, os.environ.get("OTF_TASK_ID"), self.TASK_TYPE
        )
        self.aws_access_key_id: str | None = None
        self.aws_secret_access_key: str | None = None
        self.region_name: str | None = None
        self.temporary_creds_expiry: datetime | None = None

        super().__init__(spec)

        set_aws_creds(self)

        # Use boto3 to setup the required object for the transfer
        kwargs = {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "region_name": self.region_name,
        }
        # If there's an override for endpoint_url in the environment, then use that
        kwargs2 = {}
        if os.environ.get("AWS_ENDPOINT_URL"):
            kwargs2["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL")

        self.session = boto3.session.Session(**kwargs)

        self.get_s3_client()

    def check_credential_expiry(self) -> None:
        """Check the expiry of the temporary credentials."""
        if self.temporary_creds_expiry:
            if self.temporary_creds_expiry < datetime.now(tz=tzlocal()):
                self.logger.info("Renewing temporary credentials")
                self.get_s3_client()

    def get_s3_client(self) -> None:
        """Get the temporary credentials, or just return the client."""
        # If there's an override for endpoint_url in the environment, then use that
        kwargs2 = {}
        if os.environ.get("AWS_ENDPOINT_URL"):
            kwargs2["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL")

        self.sts_client = self.session.client("sts", **kwargs2)

        if self.assume_role_arn:
            self.logger.info(f"Assuming role: {self.assume_role_arn}")
            assumed_role_object = self.sts_client.assume_role(
                RoleArn=self.assume_role_arn,
                RoleSessionName=f"OTF{time()}",
            )
            temporary_creds = assumed_role_object["Credentials"]
            # Set the credentials
            self.aws_access_key_id = temporary_creds["AccessKeyId"]
            self.aws_secret_access_key = temporary_creds["SecretAccessKey"]
            self.temporary_creds_expiry = temporary_creds["Expiration"]

            # Set these in the session
            self.session = boto3.session.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=temporary_creds["SessionToken"],
                region_name=self.region_name,
            )

        self.s3_client = self.session.client("s3", **kwargs2)

    def supports_direct_transfer(self) -> bool:
        """Return True, as you can do bucket to bucket transfers."""
        return True

    def handle_post_copy_action(self, files: list[str]) -> int:
        """Handle the post copy action specified in the config.

        Args:
            files (list[str]): A list of files that need to be handled.

        Returns:
            int: 0 if successful, 1 if not.
        """
        # Check that our creds are valid
        self.check_credential_expiry()

        # Determine the action to take
        # Delete the files
        if self.spec["postCopyAction"]["action"] == "delete":
            self.logger.info(f"Deleting files: {files}")
            response = self.s3_client.delete_objects(
                Bucket=self.spec["bucket"],
                Delete={
                    "Objects": [{"Key": file} for file in files],
                    "Quiet": True,
                },
            )

            # Check response for errors
            if response.get("Errors"):
                self.logger.error(response)
                return 1

            # Verify the files have been deleted
            for file in files:
                try:
                    response = self.s3_client.head_object(
                        Bucket=self.spec["bucket"], Key=file
                    )
                    self.logger.error(response)
                    self.logger.error(f"Failed to delete file: {file}")
                    return 1
                except ClientError as e:
                    # If it's a 404 then its good
                    if e.response["Error"]["Code"] == "404":
                        continue
                    # Otherwise, it's an error
                    self.logger.exception(e)
                    return 1

        # Copy the files to the new location, and then delete the originals
        if (
            self.spec["postCopyAction"]["action"] == "move"
            or self.spec["postCopyAction"]["action"] == "rename"
        ):
            for file in files:
                new_file = (
                    f"{self.spec['postCopyAction']['destination']}{file.split('/')[-1]}"
                )
                if self.spec["postCopyAction"]["action"] == "rename":
                    # Use the pattern and sub values to rename the file correctly
                    new_file = re.sub(
                        self.spec["postCopyAction"]["pattern"],
                        self.spec["postCopyAction"]["sub"],
                        new_file,
                    )

                self.logger.info(f'"Moving" file from {file} to {new_file}')

                self.s3_client.copy_object(
                    Bucket=self.spec["bucket"],
                    CopySource={
                        "Bucket": self.spec["bucket"],
                        "Key": file,
                    },
                    Key=new_file,
                )

                # Check that the copy worked
                try:
                    self.s3_client.head_object(Bucket=self.spec["bucket"], Key=new_file)
                except Exception as e:
                    # Print the exception message
                    self.logger.error(e)
                    self.logger.error(f"Failed to copy file: {file}")
                    return 1

                response = self.s3_client.delete_objects(
                    Bucket=self.spec["bucket"],
                    Delete={
                        "Objects": [{"Key": file}],
                        "Quiet": True,
                    },
                )
                # Check response for errors
                if response.get("Errors"):
                    self.logger.error(response)
                    return 1

                # Check that the delete worked
                try:
                    response = self.s3_client.head_object(
                        Bucket=self.spec["bucket"], Key=file
                    )
                    self.logger.error(response)
                    self.logger.error(f"Failed to delete file: {file}")
                    return 1
                except ClientError as e:
                    # If it's a 404 then its good
                    if e.response["Error"]["Code"] == "404":
                        continue
                    # Otherwise, it's an error
                    self.logger.exception(e)
                    return 1
        return 0

    def list_files(
        self, directory: str | None = None, file_pattern: str | None = None
    ) -> dict:
        """Return list of files that match the source definition.

        Args:
            directory (str, optional): The directory to search in. Defaults to None.
            file_pattern (str, optional): The file pattern to search for. Defaults to
            None.

        Returns:
            dict: A dict of files that match the source definition.
        """
        kwargs = {
            "Bucket": self.spec["bucket"],
            "MaxKeys": MAX_OBJECTS_PER_QUERY,
        }
        if directory:
            kwargs["Prefix"] = directory
        elif "directory" in self.spec and str(self.spec["directory"]):
            kwargs["Prefix"] = str(self.spec["directory"])

        remote_files = {}

        self.logger.info(
            f"Listing files in {self.spec['bucket']} matching {file_pattern}{' in' + (directory or '')}"
        )

        try:  # pylint: disable=too-many-nested-blocks
            while True:
                # Check that our creds are valid
                self.check_credential_expiry()
                response = self.s3_client.list_objects_v2(**kwargs)

                if response["KeyCount"]:
                    for object_ in response["Contents"]:
                        key = object_["Key"]
                        # Get the filename from the key
                        filename = key.split("/")[-1]

                        if file_pattern and not re.match(file_pattern, filename):
                            continue

                        # Also check the directory
                        if directory:
                            # Get the directory from the key (using basename)
                            file_directory = os.path.dirname(key)
                            if directory and file_directory != directory:
                                continue

                        if key.startswith("/"):
                            # Make sure that there is no directory in the key
                            # otherwise skip it too as we dont want anything in a subdir
                            # (as directory is not set)
                            continue

                        self.logger.info(f"Found file: {filename}")

                        # Get the size and modified time
                        file_attr = self.s3_client.head_object(
                            Bucket=self.spec["bucket"], Key=key
                        )

                        remote_files[key] = {
                            "size": file_attr["ContentLength"],
                            "modified_time": file_attr["LastModified"].timestamp(),
                        }

                    # This handles the pagination
                    # if the NextContinuationToken doesn't exist, then we'll break out
                    # of the loop
                    try:
                        kwargs["ContinuationToken"] = response["NextContinuationToken"]
                    except KeyError:
                        break
                break
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.error(f"Error listing files: {self.spec['bucket']}")
            self.logger.exception(e)
            raise e

        return remote_files

    def move_files_to_final_location(self, files: list[str]) -> None:
        """Not implemented for this handler."""
        raise NotImplementedError

    # When S3 is the destination
    def pull_files(self, files: list[str]) -> None:
        """Not implemented for this handler."""
        raise NotImplementedError

    def push_files_from_worker(self, local_staging_directory: str) -> int:
        """Push files from the worker to the destination server.

        Args:
            local_staging_directory (str): The local staging directory to upload the
            files from.

        Returns:
            int: 0 if successful, 1 if not.
        """
        # Check that our creds are valid
        self.check_credential_expiry()

        result = 0
        files = glob.glob(f"{local_staging_directory}/*")
        kwargs = {}
        if self.bucket_owner_full_control:
            kwargs["ACL"] = "bucket-owner-full-control"

        for file in files:
            # Strip the directory from the file
            file_name = file.split("/")[-1]
            self.logger.info(
                f"Transferring file: {file} to s3://{self.spec['bucket']}/{file_name}"
            )
            try:
                self.s3_client.upload_file(
                    file,
                    self.spec["bucket"],
                    f"{self.spec['directory']}/{file_name}",
                    ExtraArgs=kwargs,
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                self.logger.error(f"Failed to transfer file: {file}")
                self.logger.exception(e)
                result = 1

        return result

    def pull_files_to_worker(
        self, files: list[str], local_staging_directory: str
    ) -> int:
        """Pull files to the worker.

        Download files from AWS S3 to the local staging directory.

        Args:
            files (list): A list of files to download.
            local_staging_directory (str): The local staging directory to download the
            files to.

        Returns:
            int: 0 if successful, 1 if not.
        """
        # Check that our creds are valid
        self.check_credential_expiry()

        result = 0
        for file in files:
            # Strip the directory from the file
            file_name = file.split("/")[-1]
            self.logger.info(f"Downloading file: {file}")
            try:
                self.s3_client.download_file(
                    self.spec["bucket"],
                    file,
                    f"{local_staging_directory}/{file_name}",
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                self.logger.error(f"Failed to transfer file: {file}")
                self.logger.exception(e)
                result = 1

        return result

    def transfer_files(
        self,
        files: list[str],
        remote_spec: dict,
        dest_remote_handler: RemoteTransferHandler,
    ) -> int:
        """Transfer files from the source S3 bucket to the destination bucket.

        Args:
            files (dict): A dictionary of files to transfer.
            remote_spec (dict): Not used by this handler.
            dest_remote_handler (RemoteTransferHandler): The remote handler
            for the destination bucket

        Returns:
            int: 0 if successful, if not, then 1
            command.
        """
        # Check that our creds are valid
        self.check_credential_expiry()

        # Check the remote handler, if it's another S3Transfer, then it's simple
        # to do an S3 copy via boto

        result = 0
        for file in files:
            # Strip the directory from the file
            file_name = file.split("/")[-1]
            self.logger.info(
                f"Transferring file: {file} from {self.spec['bucket']} to {dest_remote_handler.spec['bucket']}"
            )
            try:
                self.s3_client.copy(
                    {
                        "Bucket": self.spec["bucket"],
                        "Key": file,
                    },
                    dest_remote_handler.spec["bucket"],
                    f"{dest_remote_handler.spec['directory']}/{file_name}",
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                self.logger.error(f"Error transferring file: {file}")
                self.logger.exception(e)
                result = 1

        return result

    def create_flag_files(self) -> int:
        """Create the flag files on the S3 bucket.

        Returns:
            int: 0 if successful, 1 if not.
        """
        # Check that our creds are valid
        self.check_credential_expiry()

        result = 0

        object_key = self.spec["flags"]["fullPath"]
        self.logger.info(f"Creating flag file: {object_key}")
        kwargs = {
            "Bucket": self.spec["bucket"],
            "Key": object_key,
        }
        if self.bucket_owner_full_control:
            kwargs["ACL"] = "bucket-owner-full-control"

        try:
            self.s3_client.put_object(**kwargs)
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.error(f"Error creating flag file: {object_key}")
            self.logger.exception(e)
            result = 1

        return result

    def tidy(self) -> None:
        """Tidy up the S3 client."""
        self.s3_client.close()


class S3Execution(RemoteExecutionHandler):
    """S3 remote execution handler.

    This is a strange one, because it's not really an execution. But it's a way to
    create flag files without having to define an entire transfer specification.
    """

    TASK_TYPE = "E"

    def tidy(self) -> None:
        """Tidy up the S3 client."""
        self.s3_client.close()

    def __init__(self, spec: dict):
        """Initialise the S3Execution handler.

        Args:
            spec (dict): The spec for the execution.
        """
        self.logger = opentaskpy.otflogging.init_logging(
            __name__, os.environ.get("OTF_TASK_ID"), self.TASK_TYPE
        )
        self.aws_access_key_id: str | None = None
        self.aws_secret_access_key: str | None = None
        self.region_name: str | None = None
        self.temporary_creds_expiry: datetime | None = None

        super().__init__(spec)

        set_aws_creds(self)

        # Use boto3 to setup the required object
        kwargs = {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "region_name": self.region_name,
        }

        self.session = boto3.session.Session(**kwargs)

        self.get_s3_client()

    def check_credential_expiry(self) -> None:
        """Check the expiry of the temporary credentials."""
        if self.temporary_creds_expiry:
            if self.temporary_creds_expiry < datetime.now(tz=tzlocal()):
                self.get_s3_client()

    def get_s3_client(self) -> None:
        """Get the temporary credentials, or just return the client."""
        # If there's an override for endpoint_url in the environment, then use that
        kwargs2 = {}
        if os.environ.get("AWS_ENDPOINT_URL"):
            kwargs2["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL")

        if self.assume_role_arn:
            self.sts_client = self.session.client("sts", **kwargs2)
            assumed_role_object = self.sts_client.assume_role(
                RoleArn=self.assume_role_arn,
                RoleSessionName=f"OTF{time()}",
            )
            temporary_creds = assumed_role_object["Credentials"]
            # Set the credentials
            self.aws_access_key_id = temporary_creds["AccessKeyId"]
            self.aws_secret_access_key = temporary_creds["SecretAccessKey"]
            self.temporary_creds_expiry = temporary_creds["Expiration"]

            # Set these in the session
            self.session = boto3.session.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=temporary_creds["SessionToken"],
                region_name=self.region_name,
            )

        self.s3_client = self.session.client("s3", **kwargs2)

    # This cannot be long running, so kill doesn't really need to do anything
    def kill(self) -> None:
        """Kill the remote process."""

    def execute(self) -> bool:
        """Execute the remote command.

        Returns:
            bool: True if the command was executed successfully, False otherwise
        """
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
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.error(f"Failed to create flag file: {object_key}")
            self.logger.exception(e)
            result = False

        return result
