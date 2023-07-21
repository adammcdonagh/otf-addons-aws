# pylint: skip-file
import pytest
from opentaskpy.config.schemas import validate_transfer_json


@pytest.fixture(scope="function")
def valid_protocol_definition():
    return {
        "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
    }


@pytest.fixture(scope="function")
def valid_transfer(valid_protocol_definition):
    return {
        "bucket": "test-bucket",
        "directory": "/src",
        "fileRegex": ".*\\.txt",
        "protocol": valid_protocol_definition,
    }


@pytest.fixture(scope="function")
def valid_destination(valid_protocol_definition):
    return {
        "bucket": "test-bucket",
        "directory": "/dest",
        "protocol": valid_protocol_definition,
    }


def test_s3_source_basic(valid_transfer):
    json_data = {
        "type": "transfer",
        "source": valid_transfer,
    }

    assert validate_transfer_json(json_data)

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
