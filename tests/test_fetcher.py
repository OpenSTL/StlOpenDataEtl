'''
test_fetcher.py
Script to test 'etl/fetcher.py'
'''
import os
import sys
import logging.config
import pytest
# Add project path to python sys.path
dirname = os.path.dirname(os.path.abspath(__file__))
proj_dir = os.path.join(dirname, '..')
sys.path.append(proj_dir)
from etl import utils
from etl.fetcher_response import FetcherResponse
from etl.fetcher import Fetcher


@pytest.mark.skip(reason="test not yet implemented")
def test_Fetcher():
    '''
    Integration Test
    '''
    # Specify source yaml to use
    src_yaml_path = 'data/sources/test_sources.yml'
    # Run fetcher
    responses = run_fetcher(src_yaml_path)
    # Validate output
    # assert False


@pytest.mark.skip(reason="test not yet implemented")
def test_Fetcher_class():
    '''
    Test Fetcher class instantiation
    '''
    pass


@pytest.mark.skip(reason="test not yet implemented")
def test_fetch():
    '''
    Test fetch() function in Fetcher class
    '''
    pass


@pytest.mark.skip(reason="test not yet implemented")
def test_fetch_all():
    '''
    Test fetch_all() function in Fetcher class
    '''
    pass


def run_fetcher(src_yaml_path):
    '''
    Run Fetcher in the same way as 'app.py'
    '''
    fetcher = Fetcher()
    src_yaml = utils.get_yaml(src_yaml_path)
    responses = fetcher.fetch_all(src_yaml)
    return responses


def setup_logging():
    '''
    Setup logger from config.ini
    '''
    # Setup logging
    logging_config = os.path.join(dirname, '../data/logger/config.ini')
    logging.config.fileConfig(logging_config)
    logger = logging.getLogger(__name__)
    return logger


if __name__ == '__main__':
    '''
    execute only if run as a script
    '''
    # Set up logger
    logger = setup_logging()

    # Parse Command Args for source yaml to use
    if len(sys.argv) == 1:
        # if not specified, use test_sources.yml
        src_yaml_path = 'data/sources/test_sources.yml'
    else:
        # use yaml specified in command args
        src_yaml_path = sys.argv[1]
    logger.info("Using source yaml: %s", src_yaml_path)

    # Run Fetcher
    logger.info("Running Fetcher standalone...")
    responses = run_fetcher(src_yaml_path)
    logger.debug(responses)
