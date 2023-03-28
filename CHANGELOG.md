# Changelog

## v0.4.0

   * Added proper JSON validation & add new tests to match
   * Altered GitHub workflow to validate existence of `CHANGELOG.md` for each PR
   * Update requirements for `opentaskpy` to `0.6.1`

## v0.3.0

   * Added execution handler to call Lambda functions - This allows multiple invocation types, either async or synchronous. Be aware that synchronous execution will block until it's completed. When being used with a batch, the batch cannot kill the running lambda function if it times out before the function is completed.

