#!/bin/bash

# Create a key in param store named test123
aws ssm put-parameter --name test123 --value "test123" --type String --overwrite