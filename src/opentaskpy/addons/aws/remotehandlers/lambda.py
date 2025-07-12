"""AWS Lambda remote handler."""

import base64
import json
from datetime import datetime, timedelta
from typing import Any

import boto3
import opentaskpy.otflogging
from botocore.config import Config
from botocore.exceptions import ClientError
from dateutil.tz import tzlocal
from opentaskpy.exceptions import InvalidConfigError
from opentaskpy.remotehandlers.remotehandler import RemoteExecutionHandler

from .creds import get_aws_client, set_aws_creds


class LambdaExecution(RemoteExecutionHandler):
    """AWS Lambda remote handler."""

    TASK_TYPE = "E"

    def tidy(self) -> None:
        """Tidy up the lambda client."""
        self.lambda_client.close()  # type: ignore[has-type]

    def __init__(self, spec: dict):
        """Initialise the LambdaExecution handler.

        Args:
            spec (dict): The spec for the execution.
        """
        # Ensure that function_arn is defined in the spec
        # This is really handled by the schema checks

        self.logger = opentaskpy.otflogging.init_logging(
            __name__, spec["task_id"], self.TASK_TYPE
        )

        self.aws_access_key_id: str | None = None
        self.aws_secret_access_key: str | None = None
        self.region_name: str | None = None

        super().__init__(spec)

        if "functionArn" not in self.spec:
            raise InvalidConfigError("functionArn not defined in spec")

        self.temporary_creds: dict | None = None
        self.assume_role_arn: str | None
        self.lambda_client: boto3.Client = None

        super().__init__(spec)

        set_aws_creds(self)

        self.credentials: dict = {
            "AccessKeyId": self.aws_access_key_id,
            "SecretAccessKey": self.aws_secret_access_key,
            "region_name": self.region_name,
        }

        self.validate_or_refresh_creds()

    def validate_or_refresh_creds(self) -> None:
        """Check the expiry of the temporary credentials, if applicable."""
        if self.lambda_client and not self.temporary_creds:
            return

        if self.temporary_creds:
            self.logger.debug(
                f"Temporary creds expire at: {self.temporary_creds['Expiration']} - Now: {datetime.now(tz=tzlocal())}"
            )

        if not self.lambda_client or (
            self.temporary_creds
            and self.temporary_creds["Expiration"]
            < datetime.now(tz=tzlocal()) + timedelta(minutes=1)
        ):

            if self.temporary_creds:
                self.logger.info("Renewing temporary credentials")

            # Set boto retries to 0 unless explicitly overridden in spec - retries will normally be handled within lambda code if required
            config_options: dict[str, Any] = {}
            if "max_attempts" in self.spec["protocol"]:
                config_options["retries"] = {
                    "max_attempts": self.spec["protocol"]["max_attempts"]
                }
                self.logger.info(
                    f"Setting max attempts to {self.spec['protocol']['max_attempts']}"
                )
            else:
                config_options["retries"] = {"max_attempts": 0}
            # If protocol has a botocoreReadTimeout set, then create a custom config with that set
            if "botocoreReadTimeout" in self.spec["protocol"]:
                config_options["read_timeout"] = self.spec["protocol"][
                    "botocoreReadTimeout"
                ]
                config_options["tcp_keepalive"] = True

            config = Config(**config_options)

            client_result = get_aws_client(
                "lambda",
                self.credentials,
                assume_role_arn=self.assume_role_arn,
                config=config,
            )
            self.temporary_creds = (
                client_result["temporary_creds"]
                if client_result["temporary_creds"]
                else None
            )
            self.lambda_client = client_result["client"]

    def kill(self) -> None:
        """Kill the lambda function.

        This doesn't do a huge amount. Lambda functions don't run for a long period of
        time anyway, so this is more a way to clean up the client nicely.
        """
        # We need to kill the running execute function
        self.tidy()
        self.logger.info("Closed Lambda client")

    def execute(self) -> bool:
        """Execute the lambda function.

        Triggers the lambda function. The synchronous invocation type will cause this
        thread to block. This can be an issue with batch jobs and their timeout. Boto3
        will timeout after 60 seconds regardless, so Lambda functions that are called
        should not be long running. Their own timeout should be less than the batch timeout
        if one is used.

        An async call will not check the status of the lambda function, only if there
        are errors with invoking it.
        """
        result = True

        function_arn = self.spec["functionArn"]
        invocation_type = self.spec.get("invocationType", "Event")
        payload = None
        if "payload" in self.spec:
            payload = self.spec["payload"]

        try:
            invoke_response = self.lambda_client.invoke(
                FunctionName=function_arn,
                InvocationType=invocation_type,
                LogType="Tail",
                Payload=json.dumps(payload),
            )

            self.logger.info(f"Got status code: {invoke_response['StatusCode']}")
            # Print the response
            self.logger.info(f"Lambda function response: {invoke_response}")

            # Print the response payload
            self.logger.info(
                f"Got the following response from Lambda invocation: {invoke_response['Payload'].read().decode('utf-8')}"
            )

            if (
                invoke_response["StatusCode"] != 200
                and invocation_type == "RequestResponse"
            ) or (invoke_response["StatusCode"] != 202 and invocation_type == "Event"):
                self.logger.error(f"Failed to run lambda function: {function_arn}")
                return False

            # Log the response if there is one
            if "LogResult" in invoke_response:
                # base64 decode the result
                log_result = base64.b64decode(invoke_response["LogResult"]).decode()

                self.logger.info(f"Lambda function log: {log_result}")

            # Also see if there's any actual result body
            if "Payload" in invoke_response:
                result_payload = invoke_response["Payload"].read()
                self.logger.debug(f"Lambda function payload: {result_payload}")

            if "FunctionError" in invoke_response:
                self.logger.error(
                    f"Lambda function returned an error: {function_arn} - AWS Exception"
                    f" message: {invoke_response['FunctionError']}"
                )
                return False

        except ClientError as e:
            self.logger.error(f"Failed to run lambda function: {function_arn}")
            self.logger.error(e)
            result = False

        return result
