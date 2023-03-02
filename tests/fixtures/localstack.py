import os

import boto3
import pytest


def get_root_dir():
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "test",
    )


@pytest.fixture(scope="session")
def root_dir():
    return get_root_dir()


@pytest.fixture(scope="session")
def docker_compose_files(root_dir):
    """Get the docker-compose.yml absolute path."""
    return [
        f"{root_dir}/docker-compose.yml",
    ]


@pytest.fixture(scope="session")
def localstack(docker_services):
    docker_services.start("localstack")
    public_port = docker_services.wait_for_service("localstack", 4566)
    url = f"http://{docker_services.docker_ip}:{public_port}"
    return url


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
