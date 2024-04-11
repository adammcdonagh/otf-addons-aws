#!/bin/env python3
# pylint: skip-file
# ruff: noqa
import time


def lambda_handler(event, context):
    time.sleep(1)
    raise Exception("This is a test exception")
