#!/bin/env python3
# ruff: noqa

import boto3


def lambda_handler(event, context):
    # Take a bucket name and file name as arguments
    bucket_name = event["bucket_name"]
    file_name = event["file_name"]

    # AWS_ENDPOINT_URL is picked up automatically by boto3 if set in the environment.
    # When running under floci, the Lambda function environment should have
    # AWS_ENDPOINT_URL pointing to the floci service (e.g. http://floci:4566).
    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=file_name, Body="this_is_a_test_file")
    print("This is some lambda output")
    print(event)
