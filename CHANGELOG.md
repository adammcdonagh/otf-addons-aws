# Changelog

# v26.5.0

- Add support for conditionals to s3 source schema

# v25.27.1

- Allow specifying a long token expiry time for assumed role credentials. This is useful for long running transfers, such as multipart uploads, where the token may expire before the transfer is complete.
- Add JSON schema validation to destination directory, and source filewatch directory for S3 transfers, similar to that for the source directory.

# v25.27.0

- Updates the Schema for ecsFargate remotehandler to allow specifying which container to apply containerOverides to, as well as the container for which to check the exit code to determine task success/failure
- Updates the logic for determining exit code and fetching cloudwatch logs in ecsfargate remotehandler

# v25.20.0

- Change BotoCore config used to initialise lambda client to default to zero retries of lambda executions - this can be override by setting `max_attempts` within the protocol

# v25.8.0

- Add lookup plugin for AWS Secrets Manager
- Add new environment variable to force failures if lookups fail - `OTF_AWS_SECRETS_LOOKUP_FAILED_IS_ERROR`

# v25.4.0

- Have lambda logs output to OTF logs prior to exiting when lambda returns a FunctionError

# v24.49.0

- Adjust logging level of lambda output into OTF logs from debug to info
- Bump devcontainer base image and pre-commit-hooks version

# v24.44.0

- Alter logging messages for lambda and s3 transfers a little

# v24.40.1

- Bump workflow action versions

# v24.40.0

- Add optional min_cache_age kwarg for secrets manager plugin. If specified, multiple requests to update the same secret within this time period will log a warning and not update the variable in secrets manager

## v24.38.0

- Use boto3 s3 client copy (instead of copy_object) when moving/renaming for post copy actions, to allow handling of large files
- Fix file watch test after text adjustment

## v24.36.1

- Adjust tests for SSM parameter lookup failure

## v24.36.0

- Adjust AWS (SSM) lookup plugin to log an error and use "UNKNOWN" for the value of failed secrets, instead of raising exception and breaking all jobs within environment

## v24.29.0

- Add workaround for botocore issue with socket options for Lambda

## v24.28.1

- Fix error in JSON schema for Lambda protocol

## v24.28.0

- Update Lambda protocol to allow overriding default `read_timeout¦ value from 60 seconds. This is needed when executing long running Lambda functions

## v24.25.2

- Allow blank postCopyAction, or "none". This can be useful when using the same job definition for multiple environments and are using a variable to control the PCA. This way you can set the variable to "none" in the environment where you don't want it to run.
- Also prevent S3 source directories from starting or ending with a /

## v24.25.1

- Add possibility for encryption to the s3 schemas

## v24.25.0

- Added new cacheable plugin equivalent to the SSM one, but for Secrets Manager

## v24.23.0

- Added new cacheable plugin to allow dynamically updated variables to be written back to SSM Parameter Store. For more detail see `open-task-framework` documentation for version 24.22.0
- Minor tweaks to SSM lookup plugin

## v24.19.0

- Removed a stray `break` in S3 file listing that was preventing fetching more than the first 1000 records
- Added new feature to allow moving a file from the source S3 bucket into another destination as a post copy action by referencing the full `s3://` path.

## v24.16.0

- Added a wider window for refreshing token. If it's within 60 seconds of expiry when checking we will refresh it, so handle instances where there's a delay renewing.

## v24.15.0

- Fix issue where too many files were being uploaded from worker
- Properly fix STS token refresh code
- Linting updates for ruff
- Added unused argument linting check back in, which would have caught point 1
- Refactor redundant code relating to sessions and STS token refreshing, making the code more generic and moving to `creds.py` instea

## v24.14.2

- `black` formatting fix

## v24.14.1

- Add test coverage for s3 file renames when using proxy transfer type

## v24.14.0

- Add destination file rename to S3 transfers

## v24.13.0

- Fix temporary token not refreshing correctly
- Implement required breaking changes from `opentaskpy` v24.13.1

## v24.10.1

- More logging updates to handle major logging change in `opentaskpy` v24.10.0

## v24.10.0

- Fix logging for S3 not printing correct destination directory name

## v24.9.1

- Improve logging for S3 transfers so as to not need DEBUG set to see anything useful

## v24.9.0

- Fix exceptions thrown my S3 commands etc not causing transfer to fail
- Update lambda and ECS invocations to handle assume role correctly
- Update all AssumeRole usage to also check the expiry of the temporary token and renew them if necessary

## v24.8.0

- Handle paramstore responses with newlines better

## v24.5.1

- Ensure that S3 listing only matches the file we want, and not ones in lower directory trees

## v24.5.0

- Bump version of opentaskpy required
- Alter logging for found files to only print ones that match regex
- Fixes tests that correctly started failing after upgrading opentaskpy version
- Additional checking to PCAs to validate that it has actually happened

## v24.4.1

- Change version numbering format
- Allow forcing S3 transfers to proxy rather than using CopyObject. This prevents permission issues when trying to do cross account transfers where also using AssumeRole.

## v0.7.1

- Ensure AssumeRole gets run and creds created correctly

## v0.7.0

- Add ability to use AssumeRole in protocol definition

## v0.6.1

- Increase timeout for lambda function creation test
- Add missing schema for S3 transfer - PostCopyAction

## v0.6.0

- Add ability to trigger Fargate tasks
- Fix minor typo in schemas to use the correct paths in `$id`

## v0.5.3

- Bump required version of `opentaskpy` to v0.13.0

## v0.5.2

- Add code coverage checks to GitHub workflow
- Fix missing coverage in tests, and related bugs in code

## v0.5.1

- Update JSON source schemas for S3 transfers.
- Add badges to `README.md`
- Add code coverage checks to GitHub workflow

## v0.5.0

- Updated linting and workflows to match `opentaskpy` repo standards
- Updated to work with latest `opentaskpy` version
- Fix lots of tests. It seems tests only work when run from an actual machine, and no longer inside a dev container. It seems localstack cannot run lambda functions nested within 2 levels of docker container.

## v0.4.0

- Added proper JSON validation & add new tests to match
- Altered GitHub workflow to validate existence of `CHANGELOG.md` for each PR
- Update requirements for `opentaskpy` to `0.6.1`

## v0.3.0

- Added execution handler to call Lambda functions - This allows multiple invocation types, either async or synchronous. Be aware that synchronous execution will block until it's completed. When being used with a batch, the batch cannot kill the running lambda function if it times out before the function is completed.
