from opentaskpy.config.schemas import validate_execution_json

valid_protocol_definition = {
    "name": "opentaskpy.addons.aws.remotehandlers.ecsfargate.FargateTaskExecution",
}

valid_execution = {
    "type": "execution",
    "clusterName": "some_cluster_name",
    "taskFamily": "opentaskpy-aws",
    "networkConfiguration": {
        "awsvpcConfiguration": {
            "subnets": [
                "subnet-0e35b35406e3b9fd6",
            ],
            "securityGroups": ["sg-0eaf9802367ef3b41"],
            "assignPublicIp": "ENABLED",
        }
    },
    "containerOverrides": {
        "command": ["echo", "TEST"],
        "environment": [
            {"name": "TEST", "value": "test_env_var"},
            {"name": "TEST2", "value": "test2"},
        ],
    },
    "cloudwatchLogGroupName": "/aws/batch/job",
    "timeout": 10,
    "protocol": valid_protocol_definition,
}


def test_fargate():
    json_data = {
        "type": "execution",
    }
    # Append properties from valid_execution onto json_data
    json_data.update(valid_execution)

    assert validate_execution_json(json_data)

    # Remove protocol
    del json_data["protocol"]
    assert not validate_execution_json(json_data)
