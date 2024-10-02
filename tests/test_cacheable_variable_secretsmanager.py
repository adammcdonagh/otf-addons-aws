# pylint: skip-file
# ruff: noqa

import logging
import os
from time import sleep

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


def test_cacheable_variable_vc_secretsmanager_min_cache_age(
    secretsmanager_client, caplog
):

    # Create a new ParamStore value
    secretsmanager_client.create_secret(
        Name="/test/min_cache_variable",
        SecretString="originalvalue",
    )

    secretsmanager_client.get_secret_value(SecretId="/test/min_cache_variable")

    assert (
        secretsmanager_client.get_secret_value(SecretId="/test/min_cache_variable")[
            "SecretString"
        ]
        == "originalvalue"
    )
    # Attempt update prior to min_cache_age passing since last update
    kwargs = {
        "name": "/test/min_cache_variable",
        "value": "newvalue",
        "min_cache_age": "60",
    }
    with caplog.at_level(logging.WARNING):
        vc_secretsmanager.run(**kwargs)
        assert "Not updating secret because secret last updated at" in caplog.text
    # Check the variable has not been updated
    assert (
        secretsmanager_client.get_secret_value(SecretId="/test/min_cache_variable")[
            "SecretString"
        ]
        == "originalvalue"
    )

    sleep(5)
    # Attempt update after min_cache_age has passed since last update
    kwargs = {
        "name": "/test/min_cache_variable",
        "value": "newvalue",
        "min_cache_age": "5",
    }
    vc_secretsmanager.run(**kwargs)

    # Check the variable has content of "newvalue"
    assert (
        secretsmanager_client.get_secret_value(SecretId="/test/min_cache_variable")[
            "SecretString"
        ]
        == "newvalue"
    )
