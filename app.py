'''
StlOpenDataEtl
'''

import os
import sys
import logging.config
from etl.constants import *
from etl import command_line_args, utils
from etl.fetcher import Fetcher
from etl.fetcher_local import FetcherLocal
from etl.extractor import Extractor
from etl.parser import Parser
from etl.transformer import Transformer
from etl.loader import Loader

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

if __name__ == '__main__':
    # Parse Command line arguments
    commandLineArgs = command_line_args.getCommandLineArgs()
    # Setup logging
    logging.config.fileConfig('data/logger/config.ini')
    logger = logging.getLogger(__name__)

    # notify user if the app will be using test or prod db
    if (commandLineArgs.db == 'prod'):
        logger.info('Using production database...')
        db_yaml = utils.get_yaml('data/database/config_prod.yml')
    else:
        logger.info('Using development database...')
        db_yaml = utils.get_yaml('data/database/config_dev.yml')
        # delete local db from previous run
        utils.silentremove(db_yaml['database_credentials']['db_name'])

    # Fetcher
    if (commandLineArgs.local_sources):
        logger.info("Using local data files: {}".format(' '.join(map(str, commandLineArgs.local_sources))))
        fetcher = FetcherLocal()
        filenames = commandLineArgs.local_sources
        responses = fetcher.fetch_all(filenames)
    else:
        fetcher = Fetcher()
        src_yaml = utils.get_yaml('data/sources/sources.yml')
        responses = fetcher.fetch_all(src_yaml)

    # Parser
    parser = Parser()
    responses = parser.parse_all(responses)

    # Extractor
    extractor = Extractor()
    entity_dict = extractor.extract_all(responses)

    # Transformer
    transform_tasks = utils.get_yaml('data/transform_tasks/transform_tasks.yml')
    transformer = Transformer()
    transformed_dict = transformer.transform_all(entity_dict, transform_tasks)

    # Loader
    loader = Loader(db_yaml)
    loader.load_all(transformed_dict)
