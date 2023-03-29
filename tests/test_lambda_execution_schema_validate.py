from opentaskpy.config.schemas import validate_execution_json

valid_protocol_definition = {
    "name": "opentaskpy.addons.aws.remotehandlers.lambda.LambdaExecution",
}

valid_execution = {
    "functionArn": "arn:aws:lambda:eu-west-1:000000000000:function:my-function",
    "invocationType": "Event",
    "protocol": valid_protocol_definition,
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
