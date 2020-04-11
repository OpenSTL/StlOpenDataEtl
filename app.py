'''
StlOpenDataEtl
'''

import os
import sys
import logging.config
from etl.constants import *
from etl import command_line_args, extractor, fetcher, fetcher_local, loader, \
parser, transformer, utils
from etl.progress_bar_manager import ProgressBarManager

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

if __name__ == '__main__':
    # Parse Command line arguments
    commandLineArgs = command_line_args.getCommandLineArgs()
    # Setup logging
    logging.config.fileConfig('data/logger/config.ini')
    logger = logging.getLogger(__name__)
    # Setup progress bar manager to keep track of multiple progress bars
    pbar_manager = ProgressBarManager()

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
        fetcher = fetcher_local.FetcherLocal()
        filenames = commandLineArgs.local_sources
        responses = fetcher.fetch_all(filenames)
    else:
        fetcher = fetcher.Fetcher()
        src_yaml = utils.get_yaml('data/sources/sources.yml')
        responses = fetcher.fetch_all(src_yaml)

    # Parser
    parser = parser.Parser()
    responses = parser.parse_all(responses)

    # Extractor
    extractor = extractor.Extractor()
    entity_dict = extractor.extract_all(responses)

    # Transformer
    transform_tasks = utils.get_yaml('data/transform_tasks/transform_tasks.yml')
    transformer = transformer.Transformer()
    transformed_dict = transformer.transform_all(entity_dict, transform_tasks)

    # Loader
    # read loader config
    loader = loader.Loader(db_yaml)
    # connect to database
    loader.connect()
    for tablename, transformed_df in transformed_dict.items():
        loader.insert(tablename, transformed_df)
