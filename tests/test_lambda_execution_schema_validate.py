from opentaskpy.config.schemas import validate_execution_json

valid_protocol_definition = {
    "name": "opentaskpy.addons.aws.remotehandlers.lambda.LambdaExecution",
}

valid_execution = {
    "functionArn": "arn:aws:lambda:eu-west-1:000000000000:function:my-function",
    "invocationType": "Event",
    "protocol": valid_protocol_definition,
}

valid_protocol_definition_with_retries = {
    "name": "opentaskpy.addons.aws.remotehandlers.lambda.LambdaExecution",
    "max_attempts": 3,
}

valid_execution_with_retries = {
    "functionArn": "arn:aws:lambda:eu-west-1:000000000000:function:my-function",
    "invocationType": "RequestResponse",
    "protocol": valid_protocol_definition_with_retries,
}


def test_lambda():
    json_data = {
        "type": "execution",
    }
    # Append properties from valid_execution onto json_data
    json_data.update(valid_execution)

    assert validate_execution_json(json_data)

    # Remove protocol
    del json_data["protocol"]
    assert not validate_execution_json(json_data)


def test_lambda_with_retries():
    json_data = {
        "type": "execution",
    }
    # Append properties from valid_execution_with_retries onto json_data
    json_data.update(valid_execution_with_retries)

    assert validate_execution_json(json_data)

    # Remove protocol
    del json_data["protocol"]
    assert not validate_execution_json(json_data)
