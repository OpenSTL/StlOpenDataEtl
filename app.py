'''
StlOpenDataEtl
'''

import os
import sys
import logging.config
from etl.constants import *
from etl import command_line_args, extractor, fetcher, fetcher_local, loader, parser, transformer, utils
import enlighten

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

if __name__ == '__main__':
    # Parse Command line arguments
    commandLineArgs = command_line_args.getCommandLineArgs()
    # Setup logging
    logging.config.fileConfig('data/logger/config.ini')
    logger = logging.getLogger(__name__)
    # Setup progress bar manager
    pbar_manager = enlighten.get_manager()

    # Fetcher
    if (commandLineArgs.local_sources):
        logger.info("Using local data files: {}".format(' '.join(map(str, commandLineArgs.local_sources))))
        fetcher = fetcher_local.FetcherLocal(pbar_manager)
        filenames = commandLineArgs.local_sources
        responses = fetcher.fetch_all(filenames)
    else:
        fetcher = fetcher.Fetcher(pbar_manager)
        src_yaml = utils.get_yaml('data/sources/sources.yml')
        responses = fetcher.fetch_all(src_yaml)

    # Parser
    parser = parser.Parser(pbar_manager)
    responses = parser.parse_all(responses)

    # Extractor
    extractor = extractor.Extractor(pbar_manager)
    entity_dict = extractor.extract_all(responses)

    # Transformer
    transform_tasks = utils.get_yaml('data/transform_tasks/transform_tasks.yml')
    transformer = transformer.Transformer(pbar_manager)
    transformed = transformer.transform_all(entity_dict, transform_tasks)

    # Loader
    db_yaml = utils.get_yaml('data/database/config.yml')
    loader = loader.Loader(db_yaml, pbar_manager)
    loader.connect()
    # TODO: insert, update tables using loader class
