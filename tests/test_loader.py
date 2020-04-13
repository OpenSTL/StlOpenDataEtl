'''
test_loader.py
Script to test 'etl/loader.py'
'''
import os, sys
import logging.config
import pytest
# Add project path to python sys.path
dirname = os.path.dirname(os.path.abspath(__file__))
proj_dir = os.path.join(dirname, '..')
sys.path.append(proj_dir)
# print(sys.path)
from etl.loader import Loader
from etl import utils, command_line_args


@pytest.mark.skip(reason="test not yet implemented")
def test_Loader():
    '''
    Integration Test
    '''
    assert False

@pytest.mark.skip(reason="test not yet implemented")
def test_Loader_class():
    '''
    Test Loader class instantiation
    '''
    pass

@pytest.mark.skip(reason="test not yet implemented")
def test_fetch():
    '''
    Test fetch() function in Loader class
    '''
    pass

@pytest.mark.skip(reason="test not yet implemented")
def test_fetch_all():
    '''
    Test fetch_all() function in Loader class
    '''
    pass


def run_loader(db_yaml,transformed_dict):
    '''
    Run Loader in the same way as 'app.py'
    '''
    loader = Loader(db_yaml)
    loader.load_all(transformed_dict)

def create_mock_transformed_dict(filenames):
    transformed_dict = dict()
    for filename in filenames:
        tablename = os.path.splitext(os.path.basename(filename))[0]
        transformed_dict[tablename] = utils.read_csv(filename)
    return transformed_dict

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

    # Create mock post-extraction dataframe dictionary
    db_yaml = utils.get_yaml('data/database/config_dev.yml')
    transformed_dict = create_mock_transformed_dict(filenames)

    # Run Loader
    logger.info("Running Loader standalone...")
    run_loader(db_yaml, transformed_dict)
