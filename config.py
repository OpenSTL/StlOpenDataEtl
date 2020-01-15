#!/usr/bin/env python3
# Python config file to generate secret.yaml for database credentials

# Execute
import yaml

print("Configuring database credentials:")

# Prompt user for credentials
hostname = input ("Enter hostname: ");
username = input ("Enter username: ")
password = input ("Enter password: ")

# Format into dictionary
credentials = dict(
    database_credentials = dict(
        db_hostname = hostname,
        db_username = username,
        db_password = password,
    )
)

# Populate config.yml
with open('config.yml', 'w') as outfile:
    yaml.dump(credentials, outfile, default_flow_style=False)
