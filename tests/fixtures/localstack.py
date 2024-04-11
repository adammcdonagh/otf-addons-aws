# pylint: skip-file
# ruff: noqa
import os

import boto3
import pytest


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
    if not github_actions():
        docker_services.start("localstack")
        port = docker_services.port_for("localstack", 4566)
        address = f"http://{docker_services.docker_ip}:{port}"
        return address
    else:
        address = "http://localhost:4566"
        return address


@pytest.fixture(scope="function")
def credentials(localstack, cleanup_credentials):
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_REGION"] = "eu-west-1"
    os.environ["AWS_ENDPOINT_URL"] = localstack


@pytest.fixture(scope="function")
def cleanup_credentials():
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        del os.environ["AWS_ACCESS_KEY_ID"]
    if os.environ.get("AWS_SECRET_ACCESS_KEY"):
        del os.environ["AWS_SECRET_ACCESS_KEY"]
    if os.environ.get("AWS_REGION"):
        del os.environ["AWS_REGION"]
    if os.environ.get("AWS_ENDPOINT_URL"):
        del os.environ["AWS_ENDPOINT_URL"]
    if os.environ.get("ASSUME_ROLE_ARN"):
        del os.environ["ASSUME_ROLE_ARN"]


@pytest.fixture(scope="function")
def ssm_client(localstack, credentials):
    kwargs = {
        "region_name": "eu-west-1",
    }
    session = boto3.session.Session(**kwargs)
    return session.client("ssm", endpoint_url=localstack)


@pytest.fixture(scope="function")
def lambda_client(localstack, credentials):
    kwargs = {
        "region_name": "eu-west-1",
    }
    session = boto3.session.Session(**kwargs)
    return session.client("lambda", endpoint_url=localstack)


@pytest.fixture(scope="function")
def s3_client(localstack, credentials):
    kwargs = {
        "region_name": "eu-west-1",
    }
    session = boto3.session.Session(**kwargs)
    return session.client("s3", endpoint_url=localstack)
