# Changelog

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
