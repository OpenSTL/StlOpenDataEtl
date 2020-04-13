'''
test_extractor.py
Script to test 'etl/extractor.py'
'''
import os
import sys
import logging.config
import pytest
# Add project path to python sys.path
dirname = os.path.dirname(os.path.abspath(__file__))
proj_dir = os.path.join(dirname, '..')
sys.path.append(proj_dir)
from tests.test_parser import create_mock_fetcher_responses, run_parser
from etl import utils, command_line_args
from etl.extractor import Extractor


@pytest.mark.skip(reason="test not yet implemented")
def test_Extractor():
    '''
    Integration Test
    '''
    # Validate output
    assert False


@pytest.mark.skip(reason="test not yet implemented")
def test_Extractor_class():
    '''
    Test Extractor class instantiation
    '''
    pass


@pytest.mark.skip(reason="test not yet implemented")
def test_fetch():
    '''
    Test fetch() function in Extractor class
    '''
    pass


@pytest.mark.skip(reason="test not yet implemented")
def test_fetch_all():
    '''
    Test fetch_all() function in Extractor class
    '''
    pass


def run_extractor(src_yaml_path):
    '''
    Run Extractor in the same way as 'app.py'
    '''
    extractor = Extractor()
    src_yaml = utils.get_yaml(src_yaml_path)
    responses = extractor.fetch_all(src_yaml)
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

    # Prase command line arguments, only parse local source args
    commandLineArgs = command_line_args.getCommandLineArgs(db=False)
    logger.info("Using local data files: {}".format(
        ' '.join(map(str, commandLineArgs.local_sources))))

    # Parse command line args for filenames
    filenames = commandLineArgs.local_sources
    # Create mock fetched responses
    mock_fetcher_responses = create_mock_fetcher_responses(filenames)

    # Run Parser
    mock_parsed_responses = run_parser(mock_fetcher_responses)

    # Run Extractor
    logger.info("Running Extractor standalone...")
    extractor = Extractor()
    entity_dict = extractor.extract_all(mock_parsed_responses)

    # List tables and their attributes
    for key, df in entity_dict.items():
        # filename of dataframe
        filename = 'src/'+key+'.csv'
        logger.info("Saving table %s to %s", key, filename)
        utils.to_csv(df, filename)
