# pylint: skip-file
# ruff: noqa

import os

import pytest
from botocore.exceptions import ClientError
from opentaskpy.exceptions import CachingPluginError

from opentaskpy.variablecaching.aws import vc_ssm
from tests.fixtures.localstack import *  # noqa: F403


def test_cacheable_variable_ssm_args():
    # Test combinations of the plugin with invalid args
    with pytest.raises(CachingPluginError):
        vc_ssm.run()

    with pytest.raises(CachingPluginError):
        vc_ssm.run(name="/test/variable")

    with pytest.raises(CachingPluginError):
        vc_ssm.run(value="newvalue")


def test_cacheable_variable_ssm(ssm_client):

    # Create a new ParamStore value
    ssm_client.put_parameter(
        Name="/test/variable",
        Value="originalvalue",
        Type="SecureString",
        Overwrite=True,
    )

    param = ssm_client.get_parameter(Name="/test/variable", WithDecryption=True)

    spec = {"task_id": "1234", "x": {"y": "value"}}

    kwargs = {"name": f"/test/variable", "value": "newvalue"}

    vc_ssm.run(**kwargs)

    # Check the variable has content of "newvalue"
    assert (
        ssm_client.get_parameter(Name="/test/variable", WithDecryption=True)[
            "Parameter"
        ]["Value"]
        == "newvalue"
    )


def test_cacheable_variable_ssm_failure(ssm_client):

    spec = {"task_id": "1234", "x": {"y": "value"}}

    kwargs = {"name": f"xxxx", "value": "newvalue"}

    # Remove the AWS_ENDPOINT_URL env var, so it tries to go to something that doesn't exist
    del os.environ["AWS_ENDPOINT_URL"]

    with pytest.raises(ClientError):
        vc_ssm.run(**kwargs)
