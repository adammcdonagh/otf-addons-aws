# pylint: skip-file
# ruff: noqa
import json
import logging
from json.decoder import JSONDecodeError

import opentaskpy.otflogging
import pytest
from botocore.exceptions import ClientError
from opentaskpy.config.loader import ConfigLoader
from opentaskpy.exceptions import LookupPluginError
from pytest_shell import fs

from opentaskpy.plugins.lookup.aws.secrets_manager import run
from tests.fixtures.localstack import *  # noqa: F403

PLUGIN_NAME = "secretsmanager"

# Get the default logger and set it to DEBUG
logger = logging.getLogger()
logger.setLevel(12)


def test_secrets_manager_plugin_missing_name():
    with pytest.raises(Exception) as ex:
        run()

    assert (
        ex.value.args[0]
        == f"Missing kwarg: 'name' while trying to run lookup plugin '{PLUGIN_NAME}'"
    )


def test_secrets_manager_plugin_secret_not_found(credentials, caplog):
    # Ensure lookups throw an exception when lookups fail

    os.environ["OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR"] = "1"

    with pytest.raises(ClientError) as ex:
        result = run(name="does_not_exist")

    # Ensure secrets lookup failures return "LOOKUP_FAILED" and log warning rather than raising an exception to prevent full environment failure,
    # when OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR env var is not set

    del os.environ["OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR"]

    with caplog.at_level(logging.WARNING):
        result = run(name="does_not_exist")
    assert "Secrets Manager lookup failed but continuing anyway" in caplog.text
    assert result == "LOOKUP_FAILED"


def test_secrets_manager_plugin_standard_string(secrets_manager_client):
    expected = "test1234"
    # Populate a Secrets Manager secret with a test value
    secrets_manager_client.create_secret(Name="standard_string", SecretString=expected)

    result = run(name="standard_string")

    assert result == expected


def test_secrets_manager_plugin_redacted_secret(secrets_manager_client, caplog):
    expected = " private "
    # Populate a Secrets Manager secret with a test value
    secrets_manager_client.create_secret(
        Name="redacted_secret", SecretString=" private "
    )

    with caplog.at_level(12):
        # Ensure the logger is set to the same VERBOSE level
        opentaskpy.otflogging.init_logging(
            "opentaskpy.plugins.lookup.aws.secrets_manager"
        )
        result = run(name="redacted_secret")
    assert "Read 'REDACTED' from param redacted_secret" in caplog.text

    assert result == expected


def test_config_loader_using_secretsmanager_plugin(secrets_manager_client, tmpdir):
    json_obj = {
        "testLookup": "{{ lookup('aws.secrets_manager', name='standard_string_multiline') }}",
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

    # Insert the param into secrets manager
    secrets_manager_client.create_secret(
        Name="standard_string_multiline", SecretString=expected_result
    )

    # Test that the global variables are loaded correctly
    config_loader = ConfigLoader(tmpdir)
    config_loader._load_global_variables()
    config_loader._resolve_templated_variables()

    assert config_loader.get_global_variables()["testLookup"] == expected_result


def test_config_loader_using_secretsmanager_plugin_json_path(
    secrets_manager_client, tmpdir
):
    json_obj = {
        "testLookup": "{{ lookup('aws.secrets_manager', name='plugin_json_path', value='foo.bar.[1]') }}",
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
    expected_result = "nested_secret_value"

    # Insert the param into paramstore
    secrets_manager_client.create_secret(
        Name="plugin_json_path",
        SecretString=json.dumps(
            {"foo": {"bar": ["first_secret_value", "nested_secret_value"]}}
        ),
    )

    # Test that the global variables are loaded correctly
    config_loader = ConfigLoader(tmpdir)
    config_loader._load_global_variables()
    config_loader._resolve_templated_variables()

    assert config_loader.get_global_variables()["testLookup"] == expected_result


def test_config_loader_using_secretsmanager_plugin_invalid_return_types(
    secrets_manager_client, tmpdir, caplog
):
    # Insert the param into paramstore
    secrets_manager_client.create_secret(
        Name="invalid_return_types",
        SecretString=json.dumps(
            {"foo": {"bar": ["first_secret_value", "nested_secret_value"]}}
        ),
    )

    json_obj = {
        "testLookup": "{{ lookup('aws.secrets_manager', name='invalid_return_types', value='foo') }}",
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
    # Test that the global variables are loaded correctly
    with caplog.at_level(logging.WARNING):
        config_loader = ConfigLoader(tmpdir)
        config_loader._load_global_variables()
        config_loader._resolve_templated_variables()

    assert (
        "JSONPath returned a value of type <class 'dict'>. Continuing anyway."
        in caplog.text
    )
    assert config_loader.get_global_variables()["testLookup"] == "LOOKUP_FAILED"

    # Run again but with exceptions being raised
    os.environ["OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR"] = "1"
    with pytest.raises(LookupPluginError) as ex:
        config_loader = ConfigLoader(tmpdir)
        config_loader._load_global_variables()
        config_loader._resolve_templated_variables()

    del os.environ["OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR"]

    # Delete the previous variables file
    os.remove(f"{tmpdir}/variables.json.j2")

    # Now do a list
    json_obj = {
        "testLookup": "{{ lookup('aws.secrets_manager', name='invalid_return_types', value='foo.bar') }}",
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

    with caplog.at_level(logging.WARNING):
        config_loader = ConfigLoader(tmpdir)
        config_loader._load_global_variables()
        config_loader._resolve_templated_variables()

    assert (
        "JSONPath returned a list of length 2. Returning only the first element"
        in caplog.text
    )
    assert config_loader.get_global_variables()["testLookup"] == "first_secret_value"


def test_config_loader_using_secretsmanager_plugin_invalid_json(
    secrets_manager_client, tmpdir, caplog
):
    # Insert the param into paramstore
    secrets_manager_client.create_secret(
        Name="invalid_return_json",
        SecretString=json.dumps(
            {"foo": {"bar": ["first_secret_value", "nested_secret_value"]}}
        )
        + "}}}}",  # Intentionally break the JSON
    )

    json_obj = {
        "testLookup": "{{ lookup('aws.secrets_manager', name='invalid_return_json', value='foo') }}",
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
    # Test that the global variables are loaded correctly
    with caplog.at_level(logging.WARNING):
        config_loader = ConfigLoader(tmpdir)
        config_loader._load_global_variables()
        config_loader._resolve_templated_variables()

    assert "Secret JSON parsing failed but continuing anyway" in caplog.text
    assert config_loader.get_global_variables()["testLookup"] == "LOOKUP_FAILED"

    # Run again but with exceptions being raised
    os.environ["OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR"] = "1"
    with pytest.raises(JSONDecodeError) as ex:
        config_loader = ConfigLoader(tmpdir)
        config_loader._load_global_variables()
        config_loader._resolve_templated_variables()
