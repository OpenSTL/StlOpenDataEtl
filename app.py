'''
StlOpenDataEtl
'''

import os
from etl import fetcher, parser, loader, utils

CSV = '.csv'  # comma separated values
MDB = '.mdb'  # microsoft access database (jet, access, etc.)
DBF = '.dbf'  # dbase
SHP = '.shp'  # shapes

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SUPPORTED_FILE_EXT = [CSV, MDB, DBF, SHP]

if __name__ == '__main__':
    # Fetcher
    fetcher = fetcher.Fetcher()
    src_yaml = utils.get_yaml('data/sources/sources.yml')
    responses = fetcher.fetch_all(src_yaml)

    # Parser
    parser = parser.Parser()
    for response in responses:
        try:
            response.payload = parser.flatten(response, SUPPORTED_FILE_EXT)
        except Exception as err:
            print(err)

    # Extractor
    for response in responses:
        for payload in response.payload:
            if utils.get_file_ext(payload.filename) == CSV:
                print(CSV)
            elif utils.get_file_ext(payload.filename) == MDB:
                print(MDB)
            elif utils.get_file_ext(payload.filename) == DBF:
                print(DBF)
            elif utils.get_file_ext(payload.filename) == SHP:
                print(SHP)

    # Transformer
    for response in responses:
        pass


    # Loader
    db_yaml = utils.get_yaml('data/database/config.yml')
    loader = loader.Loader(db_yaml)
    loader.connect()
    # TODO: insert, update tables using loader class
