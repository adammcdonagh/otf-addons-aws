
![unittest status](https://github.com/adammcdonagh/otf-addons-aws/actions/workflows/unittest.yml/badge.svg?event=push) ![unittest status](https://github.com/adammcdonagh/otf-addons-aws/actions/workflows/linting.yml/badge.svg?event=push)

This repository contains addons to allow integration with AWS components via [Open Task Framework (OTF)](https://github.com/adammcdonagh/open-task-framework)

Open Task Framework (OTF) is a Python based framework to make it easy to run predefined file transfers and scripts/commands on remote machines.

These addons include several additional features:
  * A new plugin for SSM Param Store to pull dynamic variables
  * A new remotehandler to push/pull files via AWS S3