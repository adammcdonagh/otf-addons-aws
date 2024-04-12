# pylint: skip-file
# ruff: noqa
import logging
import os
from copy import deepcopy

import boto3
import botocore.exceptions
import opentaskpy.otflogging
import pytest
from opentaskpy.taskhandlers import execution

from tests.fixtures.localstack import *  # noqa: F403, F405, F401
from tests.fixtures.moto import *  # noqa: F403, F405, F401

os.environ["OTF_LOG_LEVEL"] = "DEBUG"

logger = opentaskpy.otflogging.init_logging(__name__)

logger.setLevel(logging.DEBUG)

root_dir_ = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "test",
)

fargate_task_network_configuration = {
    "awsvpcConfiguration": {
        "subnets": [
            "subnet-0e35b35406e3b9fd6",
            "subnet-0bc9452610a626e11",
            "subnet-019c1f6d89f495308",
        ],
        "securityGroups": ["sg-0eaf9802367ef3b41"],
        "assignPublicIp": "ENABLED",
    }
}

fargate_execution_task_definition = {
    "type": "execution",
    "clusterName": "test_cluster",
    "taskFamily": "opentaskpy-aws",
    "timeout": 60,
    "cloudwatchLogGroupName": "/aws/batch/job",
    "networkConfiguration": fargate_task_network_configuration,
    "containerOverrides": {
        "command": ["echo", "TEST"],
        "environment": [
            {"name": "TEST", "value": "test_env_var"},
            {"name": "TEST2", "value": "test2"},
        ],
    },
    "protocol": {
        "name": "opentaskpy.addons.aws.remotehandlers.ecsfargate.FargateTaskExecution"
    },
}


@pytest.fixture(scope="function")
def credentials_aws_dev(cleanup_credentials):

    if not os.environ.get("GITHUB_ACTIONS"):
        # Look for a .env file in the root of the project
        env_file = os.path.join(root_dir_, "../.env")
        if os.path.isfile(env_file):
            with open(env_file) as f:
                for line in f:
                    if line.startswith("#"):
                        continue
                    key, value = line.strip().split("=")
                    os.environ[key] = value

    if os.environ.get("GITHUB_ACTIONS"):
        if not os.environ.get("ECS_AWS_ACCESS_KEY_ID"):
            print("ERROR: Missing AWS creds")  # noqa: T201
            assert False

        # Read the AWS credentials from the environment
        os.environ["AWS_ACCESS_KEY_ID"] = os.environ["ECS_AWS_ACCESS_KEY_ID"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ["ECS_AWS_SECRET_ACCESS_KEY"]
        os.environ["AWS_DEFAULT_REGION"] = os.environ["ECS_AWS_DEFAULT_REGION"]
        if os.environ.get("AWS_ENDPOINT_URL"):
            del os.environ["AWS_ENDPOINT_URL"]


def create_ecs_cluster():
    session = boto3.session.Session()

    client = session.client("ecs")
    # Check to see if the ECS cluster exists
    try:
        response = client.describe_clusters(clusters=["test_cluster"])
        if response["clusters"] and response["clusters"][0]["status"] == "ACTIVE":
            return response["clusters"][0]["clusterName"]
    except botocore.exceptions.ClientError:
        pass

    response = client.create_cluster(
        clusterName="test_cluster",
        capacityProviders=["FARGATE"],
        defaultCapacityProviderStrategy=[{"capacityProvider": "FARGATE"}],
    )
    assert response["cluster"]["clusterName"] == "test_cluster"
    return response["cluster"]["clusterName"]


def create_fargate_task():
    session = boto3.session.Session()

    client = session.client("ecs")

    # Check to see if the task definition exists
    try:
        response = client.describe_task_definition(taskDefinition="opentaskpy-aws")
        if (
            response["taskDefinition"]
            and response["taskDefinition"]["status"] == "ACTIVE"
        ):
            return response["taskDefinition"]["family"]
    except botocore.exceptions.ClientError:
        pass

    response = client.register_task_definition(
        family="opentaskpy-aws",
        containerDefinitions=[
            {
                "name": "opentaskpy-aws",
                "image": "opentaskpy-aws",
                "cpu": 100,
                "memory": 100,
                "essential": True,
            }
        ],
    )

    assert response["taskDefinition"]["family"] == "opentaskpy-aws"

    return response["taskDefinition"]["family"]


def test_remote_handler(credentials):
    execution_obj = execution.Execution(
        None, "run-fargate-task", fargate_execution_task_definition
    )

    execution_obj._set_remote_handlers()

    # Validate some things were set as expected
    assert execution_obj.remote_handlers[0].__class__.__name__ == "FargateTaskExecution"


def test_run_fargate_task(credentials_aws_dev):
    create_ecs_cluster()
    create_fargate_task()

    fargate_object = execution.Execution(
        None, "run-fargate-task", fargate_execution_task_definition
    )

    # Execute the task
    assert fargate_object.run()


def test_run_fargate_task_fail(credentials_aws_dev):
    create_ecs_cluster()
    create_fargate_task()

    # Change the definition to force it to fail
    fargate_execution_task_definition_copy = deepcopy(fargate_execution_task_definition)
    fargate_execution_task_definition_copy["containerOverrides"]["command"] = [
        "task-run",
    ]

    execution_obj = execution.Execution(
        None, "call-fargate-task", fargate_execution_task_definition_copy
    )

    # Execute the task
    assert not execution_obj.run()


# @mock_ecs
# def test_run_mocked_fargate_task(cleanup_credentials, credentials_moto):
#     create_ecs_cluster()
#     create_fargate_task()

#     # Manually create a FargateTaskExecution object
#     fargate_task_execution = FargateTaskExecution(fargate_execution_task_definition)

#     # Patch the boto3 client with a mocked one
#     fargate_task_execution.ecs_client = boto3.client("ecs")

#     # Execute the task. This will always timeout, because the mock never changes from
#     # RUNNING status
#     assert not fargate_task_execution.execute()
