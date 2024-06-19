# pylint: skip-file
# ruff: noqa

import os

import pytest
from botocore.exceptions import ClientError
from opentaskpy.exceptions import CachingPluginError

from opentaskpy.variablecaching.aws import vc_secretsmanager
from tests.fixtures.localstack import *  # noqa: F403


def test_cacheable_variable_ssm_args():
    # Test combinations of the plugin with invalid args
    with pytest.raises(CachingPluginError):
        vc_secretsmanager.run()

    with pytest.raises(CachingPluginError):
        vc_secretsmanager.run(name="/test/variable")

    with pytest.raises(CachingPluginError):
        vc_secretsmanager.run(value="newvalue")


def test_cacheable_variable_vc_secretsmanager(secretsmanager_client):

    # Create a new ParamStore value
    secretsmanager_client.create_secret(
        Name="/test/variable",
        SecretString="originalvalue",
    )

    secretsmanager_client.get_secret_value(SecretId="/test/variable")

    assert (
        secretsmanager_client.get_secret_value(SecretId="/test/variable")[
            "SecretString"
        ]
        == "originalvalue"
    )

    kwargs = {"name": "/test/variable", "value": "newvalue"}
    vc_secretsmanager.run(**kwargs)

    # Check the variable has content of "newvalue"
    assert (
        secretsmanager_client.get_secret_value(SecretId="/test/variable")[
            "SecretString"
        ]
        == "newvalue"
    )


def test_cacheable_variable_secretsmanager_failure(secretsmanager_client):

    kwargs = {"name": "xxxx", "value": "newvalue"}

    # Remove the AWS_ENDPOINT_URL env var, so it tries to go to something that doesn't exist
    del os.environ["AWS_ENDPOINT_URL"]

    with pytest.raises(ClientError):
        vc_secretsmanager.run(**kwargs)
