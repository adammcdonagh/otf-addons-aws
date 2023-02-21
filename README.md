
![unittest status](https://github.com/adammcdonagh/otf-addons-aws/actions/workflows/unittest.yml/badge.svg?event=push) ![unittest status](https://github.com/adammcdonagh/otf-addons-aws/actions/workflows/linting.yml/badge.svg?event=push)

This repository contains addons to allow integration with AWS components via [Open Task Framework (OTF)](https://github.com/adammcdonagh/open-task-framework)

Open Task Framework (OTF) is a Python based framework to make it easy to run predefined file transfers and scripts/commands on remote machines.

These addons include several additional features:
  * A new plugin for SSM Param Store to pull dynamic variables
  * A new remotehandler to push/pull files via AWS S3

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
   * `move` & `delete` post copy actions (TODO)
   * Touching empty files with no source. e.g. `.fin` files used as completion flags (TODO)

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