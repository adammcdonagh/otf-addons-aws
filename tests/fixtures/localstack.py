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


@pytest.fixture(scope="session")
def floci(docker_services) -> str:
    if not github_actions():
        docker_services.start("floci")
        port = docker_services.port_for("floci", 4566)
        address = f"http://{docker_services.docker_ip}:{port}"
        return address
    else:
        address = "http://localhost:4566"
        return address


@pytest.fixture(scope="function")
def credentials(floci, cleanup_credentials):
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_REGION"] = "eu-west-1"
    os.environ["AWS_ENDPOINT_URL"] = floci


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
    if os.environ.get("AWS_ASSUME_ROLE_EXTERNAL_ID"):
        del os.environ["AWS_ASSUME_ROLE_EXTERNAL_ID"]
    if os.environ.get("OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR"):
        del os.environ["OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR"]


@pytest.fixture(scope="function")
def ssm_client(floci, credentials):
    kwargs = {
        "region_name": "eu-west-1",
    }
    session = boto3.session.Session(**kwargs)
    return session.client("ssm", endpoint_url=floci)


@pytest.fixture(scope="function")
def secrets_manager_client(floci, credentials):
    kwargs = {
        "region_name": "eu-west-1",
    }
    session = boto3.session.Session(**kwargs)
    return session.client("secretsmanager", endpoint_url=floci)


@pytest.fixture(scope="function")
def lambda_client(floci, credentials):
    kwargs = {
        "region_name": "eu-west-1",
    }
    session = boto3.session.Session(**kwargs)
    return session.client("lambda", endpoint_url=floci)


@pytest.fixture(scope="function")
def s3_client(floci, credentials):
    kwargs = {
        "region_name": "eu-west-1",
    }
    session = boto3.session.Session(**kwargs)
    return session.client("s3", endpoint_url=floci)


@pytest.fixture(scope="function")
def secretsmanager_client(floci, credentials):
    kwargs = {
        "region_name": "eu-west-1",
    }
    session = boto3.session.Session(**kwargs)
    return session.client("secretsmanager", endpoint_url=floci)
