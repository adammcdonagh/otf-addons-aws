
![unittest status](https://github.com/adammcdonagh/otf-addons-aws/actions/workflows/unittest.yml/badge.svg?event=push) ![linting status](https://github.com/adammcdonagh/otf-addons-aws/actions/workflows/linting.yml/badge.svg?event=push)

This repository contains addons to allow integration with AWS components via [Open Task Framework (OTF)](https://github.com/adammcdonagh/open-task-framework)

Open Task Framework (OTF) is a Python based framework to make it easy to run predefined file transfers and scripts/commands on remote machines.

These addons include several additional features:
  * A new plugin for SSM Param Store to pull dynamic variables
  * A new remotehandler to push/pull files via AWS S3
  * A new remote handler to trigger AWS Lambda functions

# AWS Credentials

This package uses boto3 to communicate with AWS. 

Credentials can be set via config using equivalently named variables alongside the protocol definition e.g;
```json
"protocol": {
    "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
    "access_key_id": "some_key",
    "secret_access_key": "some_secret_key",
    "region_name": "eu-west-1"
}
```

If the standard AWS environment variables are set, then these will be used if not set elsewhere. Otherwise, if running from AWS, then the IAM role of the machine running OTF will be used.

# Transfers

Transfers are defined the same as a normal SSH based transfer.

As part of the upload, the `bucket-owner-full-control` ACL flag is applied to all files. This can be disabled by setting `disableBucketOwnerControlACL` to `true` in the `protocol` definition

### Supported features
   * Plain file watch
   * File watch/transfer with file size and age constraints
   * `move` & `delete` post copy actions
   * Touching empty files after transfer. e.g. `.fin` files used as completion flags
   * Touching empty files as an execution

### Limitations
   * No support for log watch

# Configuration

JSON configs for transfers can be defined as follows:

## Example File Watch Only

```json
"source": {
  "bucket": "test-bucket",
  "fileWatch": {
    "timeout": 15,
    "directory": "src",
    "fileRegex": ".*\\.txt"
  },
  "protocol": {
    "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"
  }
}
```

## Example S3 Download

```json
"source": {
  "bucket": "some-bucket",
  "directory": "src",
  "fileRegex": ".*\\.txt",
  "protocol": {
    "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"
  }
}
```

## Example S3 Upload
```json
"destination": [
    {
        "bucket": "some-bucket",
        "directory": "dest",
        "protocol": {
          "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"
        }
    }
]
```

## Example S3 upload with flag files
```json
"destination": [
    {
        "bucket": "some-bucket",
        "directory": "dest",
        "flag": {
          "fullPath": "dest/some_fin.flg"
        }
        "protocol": {
          "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"
        }
    }
]
```

# Executions

The Lambda remote handler allows AWS Lambda functions to be called. When provided with a `functionArn` the function will be called with no parameters. If there's a payload to pass in, use the `payload` attribute in the execution definition to specify a JSON object to pass into the function.

## Asynchronous vs Synchronous Execution

Lambda functions can be called with either an `invocationType` of `Event` (default if not specified) or `RequestResponse`. 

`Event` is asynchronous, and tells the Lambda function to trigger, but does not check that it ran successfully. This means it's up to you to make sure that you have appropriate monitoring of your Lambda functions.

`RequestResponse` will block until the Lambda function either completes, or times out. Boto3 has a timeout of 60 seconds, so this cannot be used for long running functions (over 1 minute). This also causes issues when used in conjunction with batches and timeouts. Since the request blocks, the thread cannot be killed by the batch thread, meaning that it will block any further execution until 60 seconds after triggering the lambda function.


## Example S3 Execution touch flag file
```json
{
  "type": "execution",
  "bucket": "test-bucket",
  "key": "test_key.flg",
  "protocol": {
    "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"
  }
}
```

## Example Lambda function call
```json
{
  "type": "execution",
  "functionArn": "arn:aws:lambda:eu-west-1:000000000000:function:my-function",
  "invocationType": "Event",
  "payload": {
    "file-name": "some_file.txt"
  },
  "protocol": {
    "name": "opentaskpy.addons.aws.remotehandlers.lambda.LambdaExecution"
  }
}
```