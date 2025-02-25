# pylint: skip-file
# ruff: noqa
import json
import logging

import pytest
from botocore.exceptions import ClientError
from opentaskpy.config.loader import ConfigLoader
from pytest_shell import fs

from opentaskpy.plugins.lookup.aws.ssm import run
from tests.fixtures.localstack import *  # noqa: F403

PLUGIN_NAME = "ssm"

# Get the default logger and set it to DEBUG
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def test_ssm_plugin_missing_name():
    with pytest.raises(Exception) as ex:
        run()

    assert (
        ex.value.args[0]
        == f"Missing kwarg: 'name' while trying to run lookup plugin '{PLUGIN_NAME}'"
    )


def test_ssm_plugin_param_name_not_found(credentials, caplog):
    os.environ["OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR"] = "1"

    with pytest.raises(ClientError) as ex:
        result = run(name="does_not_exist")

    del os.environ["OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR"]

    # Ensure parameter lookup failures return "LOOKUP_FAILED" and log warning rather than raising an exception to prevent full environment failure,
    # when OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR env var is not set
    with caplog.at_level(logging.WARNING):
        result = run(name="does_not_exist")
    assert "SSM parameter lookup failed but continuing anyway" in caplog.text
    assert result == "LOOKUP_FAILED"


def test_ssm_plugin_standard_string(ssm_client):
    expected = "test1234"
    # Populate the SSM parameter store with a test value
    ssm_client.put_parameter(
        Name="my_test_param",
        Value=expected,
        Type="String",
        Overwrite=True,
    )

    result = run(name="my_test_param")

    assert result == expected


def test_ssm_plugin_secure_string(ssm_client):
    expected = "securetest1234"
    # Populate the SSM parameter store with a test value
    ssm_client.put_parameter(
        Name="my_secure_test_param",
        Value=expected,
        Type="SecureString",
        Overwrite=True,
    )

    result = run(name="my_secure_test_param")

    assert result == expected


def test_config_loader_using_ssm_plugin(ssm_client, tmpdir):
    json_obj = {
        "testLookup": "{{ lookup('aws.ssm', name='my_test_param') }}",
    }

    fs.create_files(
        [
            {
                f"{tmpdir}/variables.json.j2": {
                    "content": json.dumps(json_obj),
                }
            },
        ]
    )
    # Test with a multi line string to make sure that it doesn't break the parser
    expected_result = """config_loader_test_1234\\nanother_line"""

    # Insert the param into paramstore
    ssm_client.put_parameter(
        Name="my_test_param",
        Value=expected_result,
        Type="String",
        Overwrite=True,
    )

    # Test that the global variables are loaded correctly
    config_loader = ConfigLoader(tmpdir)
    config_loader._load_global_variables()
    config_loader._resolve_templated_variables()

    assert config_loader.get_global_variables()["testLookup"] == expected_result
