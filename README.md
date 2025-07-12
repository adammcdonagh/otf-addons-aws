[![PyPi](https://img.shields.io/pypi/v/otf-addons-aws.svg)](https://pypi.org/project/otf-addons-aws/)
![unittest status](https://github.com/adammcdonagh/otf-addons-aws/actions/workflows/test.yml/badge.svg)
[![Coverage](https://img.shields.io/codecov/c/github/adammcdonagh/otf-addons-aws.svg)](https://codecov.io/gh/adammcdonagh/otf-addons-aws)
[![License](https://img.shields.io/github/license/adammcdonagh/otf-addons-aws.svg)](https://github.com/adammcdonagh/otf-addons-aws/blob/master/LICENSE)
[![Issues](https://img.shields.io/github/issues/adammcdonagh/otf-addons-aws.svg)](https://github.com/adammcdonagh/otf-addons-aws/issues)
[![Stars](https://img.shields.io/github/stars/adammcdonagh/otf-addons-aws.svg)](https://github.com/adammcdonagh/otf-addons-aws/stargazers)

This repository contains addons to allow integration with AWS components via [Open Task Framework (OTF)](https://github.com/adammcdonagh/open-task-framework)

Open Task Framework (OTF) is a Python based framework to make it easy to run predefined file transfers and scripts/commands on remote machines.

These addons include several additional features:

- A new plugin for SSM Param Store to pull dynamic variables
- A new plugin for AWS Secrets Manager to pull dynamic variables
- A new remotehandler to push/pull files via AWS S3
- A new remote handler to trigger AWS Lambda functions

# AWS Credentials

This package uses boto3 to communicate with AWS.

Credentials can be set via config using equivalently named variables alongside the protocol definition e.g;

```json
"protocol": {
    "name": "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer",
    "access_key_id": "some_key",
    "secret_access_key": "some_secret_key",
    "assume_role_arn": "arn:aws:iam::000000000000:role/some_role",
    "region_name": "eu-west-1"
}
```

If the standard AWS environment variables are set, then these will be used if not set elsewhere. Otherwise, if running from AWS, then the IAM role of the machine running OTF will be used.

If you are using an assumed role, the temporary credentials default to a 15 minute expiry time. This can be overridden by setting the `token_expiry_seconds` attribute in the protocol definition. The min and max values for this match the AWS STS values detailed [here](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetSessionToken.html).

# Other Environment Variables

The following environment variables can be set to override the default behaviour of the AWS remote handlers:

- `OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR`. When set to 1, will cause the lookup plugins to throw an exception when a lookup fails, otherwise they will log a warning and return `LOOKUP_FAILED` in place of the value.

# Lookup Plugins

Lookup plugins are used to pull values from external sources. The following lookup plugins are available:

- `aws.ssm` - Pulls a value from AWS SSM Parameter Store
- `aws.secrets_manager` - Pulls a value from AWS Secrets Manager, also supports JSONPath using the `jsonpath-ng` package when used with the `value` attribute

Example SSM lookup:

```json
  "access_key_id": "{{ lookup('aws.ssm', name='ssm_param_name') }}"
```

Example Secrets Manager lookup:

```json
  "access_key_id": "{{ lookup('aws.secrets_manager', name='secrets_manager_param_name', value='foo.bar.[1]') }}"
```

Will return the following, when the secret value is a JSON string in the form of:

```json
{
  "foo": {
    "bar": ["first_secret_value", "nested_secret_value"]
  }
}
```

Result: `nested_secret_value`

If the result is a list, then the first element will be returned, and a warning will be logged. The result of the JSONPath expression must be a string or an int, otherwise an error will be raised.

# Transfers

Transfers are defined the same as a normal SSH based transfer.

As part of the upload, the `bucket-owner-full-control` ACL flag is applied to all files. This can be disabled by setting `disableBucketOwnerControlACL` to `true` in the `protocol` definition

### Supported features

- Plain file watch
- File watch/transfer with file size and age constraints
- `move`, `rename` & `delete` post copy actions
- Touching empty files after transfer. e.g. `.fin` files used as completion flags
- Touching empty files as an execution

### Limitations

- No support for log watch

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
