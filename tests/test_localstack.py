# Just a simple test to run first to make sure localstack is up in the Github Actions environment
from fixtures.localstack import *  # noqa:F401


def test_localstack(localstack):
    assert localstack
