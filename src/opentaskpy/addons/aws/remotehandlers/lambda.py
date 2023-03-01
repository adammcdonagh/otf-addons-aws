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

        self.lambda_client = boto3.client("lambda", **kwargs)

    def kill(self):
        # not implemented yet (TODO)
        raise NotImplementedError

    def execute(self):
        result = True

        function_arn = self.spec["functionArn"]
        payload = None
        if "payload" in self.spec:
            payload = self.spec["payload"]

        try:
            invoke_response = self.lambda_client.invoke(
                FunctionName=function_arn,
                InvocationType="RequestResponse",  # synchronous
                Payload=json.dumps(payload),
            )

            if invoke_response["StatusCode"] != 200:
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
