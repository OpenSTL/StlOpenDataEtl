'''
StlOpenDataEtl
'''

import os
from etl import fetcher, parser, extractor, loader, utils

CSV = '.csv'  # comma separated values
DBF = '.dbf'  # dbase
MDB = '.mdb'  # microsoft access database (jet, access, etc.)
SBN = '.sbn'  # .shp support file
SBX = '.sbx'  # .shp support file
SHP = '.shp'  # shapes
SHX = '.shx'  # .shp support file

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SUPPORTED_FILE_EXT = [CSV, DBF, MDB, SBN, SBX, SHP, SHX]

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
    extractor = extractor.Extractor()
    # Master entity list
    entity_dict = dict()
    for response in responses:
        for payload in response.payload:
            if utils.get_file_ext(payload.filename) == CSV:
                entities = extractor.get_csv_data(payload)
            elif utils.get_file_ext(payload.filename) == MDB:
                entities = extractor.get_mdb_data(payload)
            elif utils.get_file_ext(payload.filename) == DBF:
                entities = extractor.get_dbf_data(payload)
            elif utils.get_file_ext(payload.filename) == SHP:
                entities = extractor.get_shp_data(response, payload)
            else:
                entities = {}
            # Add to master entity list
            entity_dict.update(entities)

    # Transformer
    for entity in entity_dict:
        print(entity)


    # Loader
    db_yaml = utils.get_yaml('data/database/config.yml')
    loader = loader.Loader(db_yaml)
    loader.connect()
    # TODO: insert, update tables using loader class
