'''
test_transformer.py
Script to test 'etl/transformer.py'
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
from etl.transformer import Transformer


@pytest.mark.skip(reason="test not yet implemented")
def test_Transformer():
    '''
    Integration Test
    '''
    assert False


@pytest.mark.skip(reason="test not yet implemented")
def test_Transformer_class():
    '''
    Test Transformer class instantiation
    '''
    pass


@pytest.mark.skip(reason="test not yet implemented")
def test_transform_all():
    '''
    Test transform_all() function in Transformer class
    '''
    pass


def run_transformer(transform_yaml_path, extracted_dict):
    '''
    Run Transformer in the same way as 'app.py'
    '''
    transform_tasks = utils.get_yaml(transform_yaml_path)
    transformer = Transformer()
    transformed_dict = transformer.transform_all(extracted_dict, transform_tasks)
    return transformed_dict

def create_mock_extractor_dict(filenames, entry_limit=100):
    # Read in csv files as DataFrame
    entity_dict = dict()
    for filename in filenames:
        tablename = os.path.splitext(os.path.basename(filename))[0]
        entity_dict[tablename]=utils.read_csv(filename)
    return entity_dict


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

    # Specify transform task yaml to use
    transform_yaml_path = 'data/transform_tasks/test_transform_tasks.yml'

    # Create mock post-extraction dataframe dictionary
    logger.info("Importing files into dataframes...")
    mock_extracted_dict = create_mock_extractor_dict(filenames)
    # logger.debug(mock_extracted_dict)

    # Run transformer
    logger.info("Running Transformer standalone...")
    transformed_dict = run_transformer(transform_yaml_path, mock_extracted_dict)
    for tablename, df in transformed_dict.items():
        utils.to_csv(df, 'src/'+tablename+'.csv')
