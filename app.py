'''
StlOpenDataEtl
'''

import os
import sys
import logging.config
import constants
from etl import progress_bar, command_line_args, extractor, fetcher, fetcher_local, loader, parser, transformer, utils
import progressbar

progressbar.streams.wrap_stderr()

CSV = '.csv'  # comma separated values
DBF = '.dbf'  # dbase
MDB = '.mdb'  # microsoft access database (jet, access, etc.)
PRJ = '.prj'  # .shp support file
SBN = '.sbn'  # .shp support file
SBX = '.sbx'  # .shp support file
SHP = '.shp'  # shapes
SHX = '.shx'  # .shp support file

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SUPPORTED_FILE_EXT = [CSV, DBF, MDB, PRJ, SBN, SBX, SHP, SHX]

if __name__ == '__main__':
    # initialize logging
    logging.config.fileConfig('data/logger/config.ini')
    logger = logging.getLogger(__name__)

    # initialize progress bar
    pbar = progress_bar.ProgressBar(100)

    # Parse Command line arguments
    commandLineArgs = command_line_args.getCommandLineArgs()

    # Fetcher
    if (commandLineArgs.local_sources):
        logger.debug('Using local data files: %s', commandLineArgs.local_sources)
        fetcher = fetcher_local.FetcherLocal(pbar,logger)
        filenames = commandLineArgs.local_sources
        responses = fetcher.fetch_all(filenames)
    else:
        fetcher = fetcher.Fetcher(pbar)
        src_yaml = utils.get_yaml('data/sources/sources.yml')
        responses = fetcher.fetch_all(src_yaml)

    # Parser
    parser = parser.Parser()
    for response in responses:
        try:
            logger.debug('Parsing %s', response.name)
            # set progress bar increment by passing in # of files to be parsed
            pbar.set_increment(len(responses))
            response.payload = parser.flatten(response, SUPPORTED_FILE_EXT)
            # update progress bar
            pbar.update()
        except Exception as err:
            logging.error(err)

    # Extractor
    extractor = extractor.Extractor()
    # Master entity list
    entity_dict = dict()
    entities = []
    for response in responses:
        # set progress bar increment by passing in # of files to be parsed
        pbar.set_increment(len(responses))
        logging.debug('Extracting %s', response.name)
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
        # update progress bar
        pbar.update()

    # Transformer
    transform_tasks = utils.get_yaml('data/transform_tasks/transform_tasks.yml')
    transformer = transformer.Transformer()
    transformed = transformer.transform_all(entity_dict, transform_tasks)

    # Loader
    db_yaml = utils.get_yaml('data/database/config.yml')
    loader = loader.Loader(db_yaml)

    loader.connect()
    # TODO: insert, update tables using loader class
