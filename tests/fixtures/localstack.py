# pylint: skip-file
import os
from sys import version

import boto3
import docker
import pytest
import pytest_localstack
import requests
from localstack.utils.bootstrap import LocalstackContainerServer


def github_actions() -> bool:
    if os.getenv("GITHUB_ACTIONS"):
        return True
    return False


@pytest.fixture(scope="session")
def root_dir() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "test",
    )


@pytest.fixture(scope="session")
def root_dir_tests() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "tests",
    )


@pytest.fixture(scope="session")
def docker_compose_files(root_dir, root_dir_tests) -> list[str]:
    """Get the docker-compose.yml absolute path."""
    return [
        f"{root_dir}/docker-compose.yml",
        f"{root_dir_tests}/docker-compose.yml",
    ]


# @pytest.fixture(scope="session")
# @pytest.mark.skipif(
#     condition=in_docker(), reason="cannot run bootstrap tests in docker"
# )

# localstack_ = pytest_localstack.session_fixture(
#     localstack_version="2.1.0",
#     services=["s3", "lambda"],  # Limit to the AWS services you need.
#     scope="session",  # Use the same Localstack container for all tests in this module.
#     autouse=True,  # Automatically use this fixture in tests.
#     region_name="eu-west-1",  # Use a specific AWS region.
# )


# @pytest.fixture(scope="session")
# def localstack() -> str:
#     return "http://localhost:4566"


@pytest.fixture(scope="session")
def localstack(docker_services) -> str:
    # os.environ["IMAGE_NAME"] = "localstack/localstack:2.1.0"
    # server = LocalstackContainerServer()
    # server.container.ports.add(4566)

    # if not server.is_up():
    #     server.start()
    #     assert server.wait_is_up(30)

    #     response = requests.get("http://localhost:4566/_localstack/health")
    #     assert response.ok, "expected health check to return OK: %s" % response.text

    # return "http://localhost:4566"

    docker_services.start("localstack")
    port = docker_services.port_for("localstack", 4566)
    address = f"http://{docker_services.docker_ip}:{port}"
    return address


@pytest.fixture(scope="session")
def credentials(localstack):
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_REGION"] = "eu-west-1"
    os.environ["AWS_ENDPOINT_URL"] = localstack


@pytest.fixture(scope="session")
def cleanup_credentials():
    del os.environ["AWS_ACCESS_KEY_ID"]
    del os.environ["AWS_SECRET_ACCESS_KEY"]
    del os.environ["AWS_REGION"]


@pytest.fixture(scope="session")
def ssm_client(localstack, credentials):
    kwargs = {
        "endpoint_url": localstack,
        "region_name": "eu-west-1",
    }
    return boto3.client("ssm", **kwargs)


@pytest.fixture(scope="session")
def lambda_client(localstack, credentials):
    kwargs = {
        "endpoint_url": localstack,
        "region_name": "eu-west-1",
    }
    return boto3.client("lambda", **kwargs)


@pytest.fixture(scope="session")
def s3_client(localstack, credentials):
    kwargs = {
        "endpoint_url": localstack,
        "region_name": "eu-west-1",
    }
    return boto3.client("s3", **kwargs)
