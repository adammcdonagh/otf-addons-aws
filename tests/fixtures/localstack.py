# pylint: skip-file
import os

import boto3
import pytest
from localstack.utils.bootstrap import LocalstackContainerServer


def github_actions() -> bool:
    if os.getenv("GITHUB_ACTIONS"):
        return True
    return False


def get_root_dir() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "test",
    )


@pytest.fixture(scope="session")
def root_dir() -> str:
    return get_root_dir()


@pytest.fixture(scope="session")
def docker_compose_files(root_dir) -> list[str]:
    """Get the docker-compose.yml absolute path."""
    return [
        f"{root_dir}/docker-compose.yml",
    ]


@pytest.fixture(scope="session")
def localstack() -> str:
    os.environ["IMAGE_NAME"] = "localstack/localstack:2.1.0"
    server = LocalstackContainerServer()
    if not server.is_up():
        server.start()
        assert server.wait_is_up(60)

    return "http://localhost:4566"


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
