#!/bin/env python3

import os

import boto3


def lambda_handler(event, context):
    # Take a bucket name and file name as arguments
    bucket_name = event["bucket_name"]
    file_name = event["file_name"]

    endpoint_url = os.environ["LOCALSTACK_HOSTNAME"]
    kwargs = {}
    if endpoint_url:
        kwargs["endpoint_url"] = f"http://{endpoint_url}:4566"

    # Connect to S3 and write a dummy file
    s3 = boto3.resource("s3", **kwargs)
    s3.Bucket(bucket_name).put_object(Key=file_name, Body="this_is_a_test_file")
