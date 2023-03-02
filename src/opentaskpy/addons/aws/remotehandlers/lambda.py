import base64
import json
import os

import boto3
import botocore.exceptions
import opentaskpy.logging
from opentaskpy.remotehandlers.remotehandler import RemoteExecutionHandler

from .aws import set_aws_creds


class LambdaExecution(RemoteExecutionHandler):
    TASK_TYPE = "E"

    def tidy(self):
        self.lambda_client.close()

    def __init__(self, spec):
        self.spec = spec

        # Ensure that function_arn is defined in the spec
        if "functionArn" not in self.spec:
            raise Exception("functionArn not defined in spec")

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

        # Set the client to only try once. This prevents the lambda function being called more than once
        # and issues when working with batch timeouts if a lambda function takes longer than the batch timeout
        # It still means that the timeout of a lambda function is always at least 60 seconds due to the way boto3's HTTP timeout works
        kwargs["config"] = botocore.client.Config(retries={"max_attempts": 0})

        self.lambda_client = boto3.client("lambda", **kwargs)

    def kill(self):
        # We need to kill the running execute function
        self.tidy()
        self.logger.info("Killing thread")

    # Triggers the lambda function. The synchronous invocation type will cause this thread
    # to block. This can be an issue with batch jobs and their timeout.
    # Boto3 will timeout after 60 seconds regardless, so Lambda functions that are called should not be long running. Their own timeout should be less than the
    # batch timeout if one is used.

    # An async call will not check the status of the lambda function, only if there are errors with invoking it.

    def execute(self):
        result = True

        function_arn = self.spec["functionArn"]
        invocation_type = (
            self.spec["invocationType"] if "invocationType" in self.spec else "Event"
        )
        payload = None
        if "payload" in self.spec:
            payload = self.spec["payload"]

        try:
            invoke_response = self.lambda_client.invoke(
                FunctionName=function_arn,
                InvocationType=invocation_type,
                Payload=json.dumps(payload),
            )

            if (
                invoke_response["StatusCode"] != 200
                and invoke_response["StatusCode"] != 202
            ):
                self.logger.error(f"Failed to run lambda function: {function_arn}")
                return False

            if "FunctionError" in invoke_response:
                self.logger.error(
                    f"Lambda function returned an error: {function_arn} - AWS Exception message: {invoke_response['FunctionError']}"
                )
                return False

            # Log the response if there is one
            if "LogResult" in invoke_response:
                # base64 decode the result
                log_result = base64.b64decode(invoke_response["LogResult"])

                self.logger.debug(f"Lambda function response: {log_result}")

        except botocore.exceptions.ClientError as e:
            self.logger.error(f"Failed to run lambda function: {function_arn}")
            self.logger.error(e)
            result = False

        return result
