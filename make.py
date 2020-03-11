#!/usr/bin/env python3
# Python make file for repeated build functions

# Execute pip freeze to collect our dependencies to a requirements.txt file
import subprocess
from sys import platform

print("Executing pip install against collected requirements.")
requirements = subprocess.Popen(
    ['pip', 'install', '-r', 'requirements.txt'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

print("Installing mdbtools...")
if platform == "linux" or platform == "linux2":
    # ubuntu
    dependencies = subprocess.Popen(
        ['sudo', 'apt-get', 'install','-y','mdbtools'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
elif platform == "darwin":
    # OS X
    dependencies = subprocess.Popen(
        ['brew', 'install', 'mdbtools'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
elif platform == "win32":
    # Windows
    dependencies = subprocess.Popen(
        ['brew', 'install', 'mdbtools'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

stdout, stderr = requirements.communicate()
print(stdout.decode('utf-8'))
