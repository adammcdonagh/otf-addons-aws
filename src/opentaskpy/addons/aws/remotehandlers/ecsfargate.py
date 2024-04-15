"""AWS Fargate Task remote handler."""

from datetime import datetime, timedelta
from time import sleep

import boto3
import opentaskpy.otflogging
from botocore.exceptions import ClientError
from dateutil.tz import tzlocal
from opentaskpy.remotehandlers.remotehandler import RemoteExecutionHandler

from .creds import get_aws_client, set_aws_creds


class FargateTaskExecution(RemoteExecutionHandler):
    """AWS Fargate Task remote handler."""

    TASK_TYPE = "E"
    fargate_task_id: str

    def tidy(self) -> None:
        """Tidy up the ecs client."""
        self.ecs_client.close()

    def __init__(self, spec: dict):
        """Initialise the FargateTaskExecution handler.

        Args:
            spec (dict): The spec for the execution.
        """
        self.logger = opentaskpy.otflogging.init_logging(
            __name__, spec["task_id"], self.TASK_TYPE
        )
        self.aws_access_key_id: str | None = None
        self.aws_secret_access_key: str | None = None
        self.region_name: str | None = None
        self.temporary_creds: dict | None = None
        self.assume_role_arn: str | None
        self.ecs_client: boto3.Client = None

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
        if self.ecs_client and not self.temporary_creds:
            return

        if self.temporary_creds:
            self.logger.debug(
                f"Temporary creds expire at: {self.temporary_creds['Expiration']} - Now: {datetime.now(tz=tzlocal())}"
            )

        if not self.ecs_client or (
            self.temporary_creds
            and self.temporary_creds["Expiration"]
            < datetime.now(tz=tzlocal()) + timedelta(minutes=1)
        ):

            if self.temporary_creds:
                self.logger.info("Renewing temporary credentials")

            client_result = get_aws_client(
                "ecs", self.credentials, self.assume_role_arn
            )
            self.temporary_creds = (
                client_result["temporary_creds"]
                if client_result["temporary_creds"]
                else None
            )
            self.ecs_client = client_result["client"]

    def kill(self) -> None:
        """Kill the fargate task function.

        Runs the stop_task function against AWS ECS. This will terminate the task.
        """
        # We need to kill the running execute function
        self.ecs_client.stop_task(
            cluster=self.spec["clusterName"],
            task=self.task_id,
            reason="Task killed by OTF",
        )

        self.tidy()
        self.logger.info("Closed ECS client")

    def execute(self) -> bool:
        """Execute the Fargate task.

        Triggers the fargate task. This will run the task, and then continue to poll the
        state of the task, until it finishes (errors or succeeds). Or, if a timeout is
        set within the task definition, it will timeout after that time.

        If a cloudwatch log group is defined in the task definition, then the logs will
        be collected and returned in the output.

        Returns:
            bool: True if the task succeeded, False if it failed.
        """
        result = True

        task = self.spec["taskFamily"]
        cluster_name = self.spec["clusterName"]
        timeout = self.spec.get("timeout", -1)
        init_timeout = self.spec.get("initTimeout", 60)

        try:
            overrides = {"containerOverrides": [self.spec["containerOverrides"]]}

            overrides["containerOverrides"][0]["name"] = "default"

            run_response = self.ecs_client.run_task(
                cluster=cluster_name,
                taskDefinition=task,
                overrides=overrides,
                networkConfiguration=self.spec["networkConfiguration"],
                launchType="FARGATE",
            )

            # Check that the task has been triggered successfully
            # Check that we got a 200 OK response
            if run_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                self.logger.error(
                    f"Failed to run fargate task: {task} in cluster {cluster_name}"
                )
                return False

            # Check that the task has been triggered successfully
            if run_response["failures"]:
                self.logger.error(
                    f"Failed to run fargate task: {task} in cluster {cluster_name}"
                )
                return False

            # Get the task id
            self.fargate_task_id = run_response["tasks"][0]["taskArn"].split("/")[-1]

            # Now we loop until the task has finished, or until we've timed out
            # We'll check the status of the task every 5 seconds
            # If the task has a timeout set, then we'll use that as the timeout
            while True:
                self.validate_or_refresh_creds()
                # Get the task status
                self.logger.info("Checking status of task")
                task_status = self.ecs_client.describe_tasks(
                    cluster=cluster_name, tasks=[self.fargate_task_id]
                )

                # Check that we got a 200 OK response
                if task_status["ResponseMetadata"]["HTTPStatusCode"] != 200:
                    self.logger.error(
                        f"Failed to get status of fargate task: {task} in cluster"
                        f" {cluster_name}"
                    )
                    result = False
                    break

                # Check the status of the task
                task_status = task_status["tasks"][0]["lastStatus"]
                self.logger.info(f"Task status: {task_status}")

                # If the task has not started running yet, start decrementing the
                # init_timer. If the init_timer reaches 0, then we can break out of the
                # loop and return False
                if task_status == "PENDING":
                    if init_timeout <= 0:
                        self.logger.error(
                            f"Task: {task} in cluster {cluster_name} timed out while"
                            " initialising."
                        )
                        result = False
                        break

                    init_timeout -= 5

                # If the task has finished, then we can break out of the loop
                if task_status in ["STOPPED", "DEPROVISIONING"]:
                    break

                # If the task has timed out, then we can break out of the loop
                if timeout != -1 and timeout <= 0:
                    self.logger.error(
                        f"Task: {task} in cluster {cluster_name} timed out"
                    )
                    result = False
                    break

                # If we haven't timed out, then we can sleep for 5 seconds
                # If the timeout is > -1 then we'll decrement the timeout
                if timeout != -1:
                    timeout -= 5

                sleep(5)

            # If we've got this far, then the task has finished. We need to check the
            # status to see if it succeeded or failed, and if necessary, also pull the
            # logs from CloudWatch Logs
            task_status = self.ecs_client.describe_tasks(
                cluster=cluster_name, tasks=[self.fargate_task_id]
            )

            if len(task_status["tasks"][0]["containers"]) == 0:
                # If there's no containers, then the task either failed, or we're
                # running in a mocked environment. Here we will fail the task
                self.logger.error(
                    f"Task: {task} in cluster {cluster_name} failed with no containers"
                )
                return False

            container_name = task_status["tasks"][0]["containers"][0]["name"]

            # Do we need to obtain logs?
            if self.spec.get("cloudwatchLogGroupName"):
                # Get the log events
                log_events = get_aws_client(
                    "logs", self.credentials, self.assume_role_arn
                )["client"].get_log_events(
                    logGroupName=self.spec["cloudwatchLogGroupName"],
                    logStreamName=f"{task}/{container_name}/{self.fargate_task_id}",
                )

                # Get the logs
                logs = []
                for event in log_events["events"]:
                    logs.append(event["message"])

                self.logger.info("Fargate Task output:")
                # Log the messages to the logger
                for line in logs:
                    self.logger.info(line)

            # Check the exitCode of the container to get the result
            exit_code = (
                1
                if "exitCode" not in task_status["tasks"][0]["containers"][0]
                else task_status["tasks"][0]["containers"][0]["exitCode"]
            )

            if exit_code != 0:
                reason = (
                    task_status["tasks"][0]["containers"][0]["reason"]
                    if "reason" in task_status["tasks"][0]["containers"][0]
                    else task_status["tasks"][0]["stoppedReason"]
                )

                self.logger.error(
                    f"Task: {task} in cluster {cluster_name} failed. Reason: {reason}"
                )
                result = False

        except ClientError as e:
            self.logger.error(
                f"Failed to run fargate task: {task} in cluster {cluster_name}"
            )
            self.logger.error(e)
            result = False

        return result
