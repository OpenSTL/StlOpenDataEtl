import yaml
import sqlalchemy
from etl.utils import xstr
from io import StringIO
import logging

class Loader:
    '''
    A class to load transformed data to database

        "_engine" - sqlalchemy engine object - connection handle to database
        "_config" - yaml object - yaml object with database credentials
    '''
    _engine = None
    _config = None
    _metadata = None

    # Initializer / Instance Attributes
    def __init__(self, config_yaml, pbar_manager):
        # Get credentials from YAML
        self.credentials = self.get_credentials(config_yaml,'database_credentials')
        self.pbar_manager = pbar_manager
        self.logger = logging.getLogger(__name__)

    # Get credentials from yaml config
    def get_credentials(self, config_yaml, match_key):
        '''
        Returns an entry from yaml that matches match_key

        Arguments:
        config_yaml -- yaml object
        match_key -- string representing key to match in yaml dictionary

        '''
        for key in config_yaml.keys():
            if key == match_key:
                return config_yaml[key]

    # Connect to remote mysql db
    def connect(self):
        # create sqlalchemy engine
        engine = sqlalchemy.create_engine("{dialect}://{user}{pw}{host}/{db}"
                               .format(dialect = xstr(self.credentials['dialect_driver']),
                                        user = xstr(self.credentials['db_username']),
                                        host = ('@', '')[self.credentials['db_hostname'] is None] + xstr(self.credentials['db_hostname']),
                                        pw = (':', '')[self.credentials['db_password'] is None] + xstr(self.credentials['db_password']),
                                        db = xstr(self.credentials['db_name'])))
        # try to connect to database
        try:
            engine.connect()
            self._engine = engine
            self._metadata = sqlalchemy.MetaData()
        except sqlalchemy.exc.OperationalError as except_detail:
            # print error details
            print("ERROR: {}".format(except_detail))
        return

    def insert(self, tablename, table_df, chunk_size = 1000):
        '''
        Inserts pandas dataframe (table_df) into SQL database.
        If table already exists, this call will drop the old table before inserting new values.

        Arguments:
        tablename - table name string
        table_df - dataframe containing table with transformed data
        chunk_size - number of rows in each batch to be written at a time
        '''
        # If table doesn't exist, Create.
        if not self._engine.dialect.has_table(self._engine, tablename):
            self.logger.debug("Inserting table %s into database %s...", tablename, self.credentials['db_name'])
            table_df.iloc[:0].to_sql(tablename, self._engine, if_exists='fail')
        # insert new table, drop old table if exists
        table_df.to_sql(name=tablename, con = self._engine, if_exists = 'replace', chunksize = chunk_size)


    def update(self, tablename, table_df, chunk_size = 1000):
        '''
        Update existing table in SQL database

        Arguments:
        tablename - table name string
        table_df - dataframe containing table with transformed data
        chunk_size - number of rows in each batch to be written at a time
        '''
        # Update matching ids with dataframe in table_name
        pass
