# pylint: skip-file
# ruff: noqa

from opentaskpy.taskhandlers import transfer

from opentaskpy.variablecaching.aws import vc_ssm
from tests.fixtures.localstack import *  # noqa: F403

dummy_task_definition = {
    "task_id": 1234,
    "type": "transfer",
    "source": {
        "accessToken": "0",
        "protocol": {"name": "dummy"},
        "cacheableVariables": [
            {
                "variableName": "accessToken",
                "cachingPlugin": "aws.vc_ssm",
                "cacheArgs": {
                    "name": "/test/variablename",
                },
            }
        ],
    },
}


def test_dummy_transfer(ssm_client):
    #  The key thing to test is that the access token
    #  is written to the cache file

    transfer_obj = transfer.Transfer(None, "dummy_task_transfer", dummy_task_definition)

    transfer_obj.run()

    # Check the parameter store value now exists and contains a random number
    param = ssm_client.get_parameter(Name="/test/variablename", WithDecryption=True)
    assert param["Parameter"]["Value"] != "0"
    assert param["Parameter"]["Value"].isdigit()
