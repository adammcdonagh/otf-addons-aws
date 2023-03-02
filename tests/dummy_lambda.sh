#!/bin/bash

# Create a file file containing the lambda_write*.py file
zip lambda.zip lambda_write*.py

# Delete test bucket
awslocal s3 rb s3://test-bucket --force

# Create a test bucket
awslocal s3 mb s3://test-bucket

# Delete function if it already exists
awslocal lambda delete-function --function-name test_function

awslocal lambda create-function \
  --function-name test_function \
  --role arn:aws:iam:0000000000:role/lambda-test \
  --runtime python3.9 \
  --handler lambda_write_to_s3.lambda_handler \
  --timeout 300 \
  --memory-size 256 \
  --zip-file fileb://lambda.zip \

# Call the function and see what happens
awslocal lambda invoke \
  --function-name test_function \
  --payload '{"bucket_name": "test-bucket", "file_name": "test_file.txt", "endpoint_url": "http://localhost:4566"}' \
  output.txt