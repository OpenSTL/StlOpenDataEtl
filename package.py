#!/usr/bin/env python3
# Python package file for repeated packaging functions

# Execute pip freeze to collect our dependencies to a requirements.txt file
import subprocess

print('Collecting requirements from project to requirements.txt')
requirements = subprocess.Popen(
    ['pip', 'freeze'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

stdout, stderr = requirements.communicate()
print(stdout.decode('utf-8'))


with open('requirements.txt', 'w') as requirementsWriter:
    # Remove invalid package 'pkg-resources==0.0.0' (Ubuntu/pip bug)
    packages = stdout.decode('utf-8').replace('pkg-resources==0.0.0','')
    requirementsWriter.write(packages)
