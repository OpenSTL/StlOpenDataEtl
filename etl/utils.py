import os
import errno
import yaml
import zipfile
import pandas as pd
from etl.payload_data import PayloadData
from io import BytesIO
from posixpath import basename
from urllib.parse import urlparse


def get_yaml(yaml_file):
    '''
    Returns a YAML object
    '''
    with open(os.path.join(yaml_file), 'r') as yamlReader:
        return yaml.load(yamlReader, Loader=yaml.FullLoader)


def get_file_name_from_uri(uri):
    parsed_path = urlparse(uri).path
    return basename(parsed_path)


def get_file_ext(path):
    return os.path.splitext(path)[1]


def decompress(archive_binary_data, supported_file_extensions=None):
    if not zipfile.is_zipfile(archive_binary_data):
        print('warning: passed argument is not an archive.')
        return archive_binary_data

    archive = zipfile.ZipFile(archive_binary_data)
    files_to_extract = []
    decompressed_files = []

    if not supported_file_extensions:
        for name in archive.namelist():
            decompressed_files.append(PayloadData(
                name, BytesIO(archive.read(name))))
        return decompressed_files

    for compressed_filename in archive.namelist():
        compressed_file_extension = get_file_ext(compressed_filename)
        if compressed_file_extension in supported_file_extensions:
            files_to_extract.append(compressed_filename)

    for name in files_to_extract:
        decompressed_files.append(PayloadData(
            name, BytesIO(archive.read(name))))
    return decompressed_files


# remove a file, suppress error if file removal fails
def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occurred

# Convert NoneType to string
def xstr(s):
    if s is None:
        return ''
    return str(s)

# Export to csv perserving datatype
def to_csv(df, filename):
    # Prepend dtypes to the top of df
    df.loc[-1] = df.dtypes
    df.index = df.index + 1
    df.sort_index(inplace=True)
    # Then save it to a csv
    df.to_csv(filename, index=False)

# Import from csv preserving datatype
def read_csv(filename):
    # Read types first line of csv
    dtypes = pd.read_csv(filename, nrows=1).iloc[0].to_dict()
    # Replace unrecognized dtype with 'object'
    for attribute,dtype in dtypes.items():
        if str(dtype) == 'geometry':
            dtypes[attribute] = 'object'
    # Read the rest of the lines with the types from above
    return pd.read_csv(filename, dtype=dtypes, skiprows=[1])
