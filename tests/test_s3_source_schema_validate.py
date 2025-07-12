# pylint: skip-file
import pytest
from opentaskpy.config.schemas import validate_transfer_json


@pytest.fixture(scope="function")
def valid_protocol_definition():
    return {
        "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
    }


@pytest.fixture(scope="function")
def valid_protocol_definition_using_keys():
    return {
        "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        "access_key_id": "12345",
        "secret_access_key": "12345",
    }


@pytest.fixture(scope="function")
def invalid_protocol_definition_using_keys_no_secret():
    return {
        "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        "access_key_id": "12345",
    }


@pytest.fixture(scope="function")
def valid_protocol_definition_using_assume_role():
    return {
        "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        "assume_role_arn": "12345",
    }


@pytest.fixture(scope="function")
def valid_protocol_definition_using_assume_role_and_keys():
    return {
        "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
        "assume_role_arn": "12345",
        "access_key_id": "12345",
        "secret_access_key": "12345",
    }


@pytest.fixture(scope="function")
def valid_transfer(valid_protocol_definition):
    return {
        "bucket": "test-bucket",
        "directory": "src",
        "fileRegex": ".*\\.txt",
        "protocol": valid_protocol_definition,
    }


@pytest.fixture(scope="function")
def valid_destination(valid_protocol_definition):
    return {
        "bucket": "test-bucket",
        "directory": "dest",
        "protocol": valid_protocol_definition,
    }


def test_s3_source_protocol(
    valid_transfer,
    valid_protocol_definition_using_assume_role,
    valid_protocol_definition_using_keys,
    invalid_protocol_definition_using_keys_no_secret,
    valid_protocol_definition_using_assume_role_and_keys,
):
    json_data = {
        "type": "transfer",
        "source": valid_transfer,
    }

    json_data["source"]["protocol"] = valid_protocol_definition_using_assume_role
    assert validate_transfer_json(json_data)

    json_data["source"]["protocol"] = valid_protocol_definition_using_keys
    assert validate_transfer_json(json_data)

    json_data["source"][
        "protocol"
    ] = valid_protocol_definition_using_assume_role_and_keys
    assert validate_transfer_json(json_data)

    json_data["source"]["protocol"] = invalid_protocol_definition_using_keys_no_secret
    assert not validate_transfer_json(json_data)

    json_data["source"]["protocol"] = valid_protocol_definition_using_assume_role

    # Set the expiry to a sensible value
    json_data["source"]["protocol"]["token_expiry_seconds"] = 10000
    assert validate_transfer_json(json_data)

    # Set it too low, and too high
    json_data["source"]["protocol"]["token_expiry_seconds"] = 899
    assert not validate_transfer_json(json_data)

    json_data["source"]["protocol"]["token_expiry_seconds"] = 43201
    assert not validate_transfer_json(json_data)

    # Remove the assume role arn and validate it fails
    del json_data["source"]["protocol"]["assume_role_arn"]
    assert not validate_transfer_json(json_data)


def test_s3_source_basic(valid_transfer):
    json_data = {
        "type": "transfer",
        "source": valid_transfer,
    }

    assert validate_transfer_json(json_data)

    # Add / to the directory and validate it fails
    json_data["source"]["directory"] = "/src/"
    assert not validate_transfer_json(json_data)

    json_data["source"]["directory"] = "/src"
    assert not validate_transfer_json(json_data)

    json_data["source"]["directory"] = "src/"
    assert not validate_transfer_json(json_data)

    # Remove protocol
    del json_data["source"]["protocol"]
    assert not validate_transfer_json(json_data)


def test_s3_source_file_watch(valid_transfer):
    json_data = {
        "type": "transfer",
        "source": valid_transfer,
    }

    json_data["source"]["fileWatch"] = {
        "timeout": 10,
        "directory": "src",
        "fileRegex": ".*\\.txt",
    }

    assert validate_transfer_json(json_data)

    # Remove fileRegex
    del json_data["source"]["fileWatch"]["fileRegex"]
    assert validate_transfer_json(json_data)

    # Remove directory
    del json_data["source"]["fileWatch"]["directory"]
    assert validate_transfer_json(json_data)

    # Add watchOnly
    json_data["source"]["fileWatch"]["watchOnly"] = True
    assert validate_transfer_json(json_data)

    # Add error
    json_data["source"]["error"] = True
    assert validate_transfer_json(json_data)


def test_s3_post_copy_action(valid_transfer):

    json_data = {
        "type": "transfer",
        "source": valid_transfer,
    }

    json_data["source"]["postCopyAction"] = {
        "action": "move",
        "destination": "s3://test-bucket/dest",
    }

    assert not validate_transfer_json(json_data)

    json_data["source"]["postCopyAction"]["destination"] = "s3://test-bucket/dest/"
    assert validate_transfer_json(json_data)

    json_data["source"]["postCopyAction"]["destination"] = "s3://"
    assert not validate_transfer_json(json_data)

    json_data["source"]["postCopyAction"]["destination"] = "archive/"
    assert validate_transfer_json(json_data)

    json_data["source"]["postCopyAction"]["destination"] = "/"
    assert validate_transfer_json(json_data)

    json_data["source"]["postCopyAction"] = {
        "action": "",
    }
    assert validate_transfer_json(json_data)

    json_data["source"]["postCopyAction"] = {
        "action": "none",
        "destination": "s3://test-bucket/dest",
    }
    assert validate_transfer_json(json_data)

    json_data["source"]["postCopyAction"] = {
        "action": "invalid",
    }
    assert not validate_transfer_json(json_data)


def test_s3_destination(valid_transfer, valid_destination):
    json_data = {
        "type": "transfer",
        "source": valid_transfer,
        "destination": [valid_destination],
    }

    assert validate_transfer_json(json_data)

    # Add flags
    json_data["destination"][0]["flags"] = {"fullPath": "flag.txt"}
    assert validate_transfer_json(json_data)

    # Remove protocol
    del json_data["destination"][0]["protocol"]
    assert not validate_transfer_json(json_data)
