#!/usr/bin/env python3
# Python config file to generate secret.yaml for database credentials

# Execute
import yaml
import errno
import os
from getpass import getpass

print("Configuring database credentials:")

# Prompt user for credentials
hostname = input ("Enter hostname: ")
db_name = input ("Enter database name: ")
username = input ("Enter username: ")
password = getpass(prompt='Enter password: ', stream=None)

# Format into dictionary
credentials = dict(
    database_credentials = dict(
        db_hostname = hostname,
        db_username = username,
        db_password = password,
        db_name = db_name
    )
)
# Make directory if doesn't already exist
filename = 'data/database/config.yml'
if not os.path.exists(os.path.dirname(filename)):
    try:
        os.makedirs(os.path.dirname(filename))
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

# Populate config.yml
with open(filename, 'w') as outfile:
    yaml.dump(credentials, outfile, default_flow_style=False)
