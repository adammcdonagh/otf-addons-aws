from opentaskpy.config.schemas import validate_transfer_json

valid_protocol_definition = {
    "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
}

valid_transfer = {
    "bucket": "test-bucket",
    "path": "/src",
    "fileRegex": ".*\\.txt",
    "protocol": {"name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"},
}

valid_destination = {
    "bucket": "test-bucket",
    "path": "/dest",
    "protocol": {"name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"},
}


def test_s3_source_basic():
    json_data = {
        "type": "transfer",
        "source": valid_transfer,
    }

    assert validate_transfer_json(json_data)

    # Remove protocol
    del json_data["source"]["protocol"]
    assert not validate_transfer_json(json_data)


def test_s3_source_file_watch():
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


def test_s3_destination():
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
