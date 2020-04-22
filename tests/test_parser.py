'''
test_parser.py
Script to test 'etl/parser.py'
'''
import os
import sys
import logging.config
import pytest
# Add project path to python sys.path
dirname = os.path.dirname(os.path.abspath(__file__))
proj_dir = os.path.join(dirname, '..')
sys.path.append(proj_dir)
from etl import utils, command_line_args
from etl.fetcher_local import FetcherLocal
from etl.parser import Parser


@pytest.mark.skip(reason="test not yet implemented")
def test_Parser():
    '''
    Integration Test
    '''
    # Specify filenames
    filenames = [proj_dir+'/src/prcl_shape.zip']
    # Create mock fetched responses
    mock_fetcher_responses = create_mock_fetcher_responses(filenames)
    # Run Parser
    responses = run_parser(mock_fetcher_responses)
    # Validate output
    assert False


@pytest.mark.skip(reason="test not yet implemented")
def test_Parser_class():
    '''
    Test Parser class instantiation
    '''
    pass


@pytest.mark.skip(reason="test not yet implemented")
def test_fetch():
    '''
    Test fetch() function in Parser class
    '''
    pass


@pytest.mark.skip(reason="test not yet implemented")
def test_fetch_all():
    '''
    Test fetch_all() function in Parser class
    '''
    pass


def run_parser(responses):
    '''
    Run Parser in the same way as 'app.py'
    '''
    parser = Parser()
    parsed_responses = parser.parse_all(responses)
    return parsed_responses


def create_mock_fetcher_responses(filenames):
    # Use FetcherLocal to create mock responses
    fetcher = FetcherLocal()
    mock_fetcher_response = fetcher.fetch_all(filenames)
    return mock_fetcher_response


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

    # Parse command line args for filenames
    commandLineArgs = command_line_args.getCommandLineArgs(db=False)
    logger.info("Using local data files: {}".format(
        ' '.join(map(str, commandLineArgs.local_sources))))
    filenames = commandLineArgs.local_sources

    # Create mock fetched responses
    mock_fetcher_responses = create_mock_fetcher_responses(filenames)

    # Run Parser
    logger.info("Running Parser standalone...")
    responses = run_parser(mock_fetcher_responses)
    logger.debug(responses)
