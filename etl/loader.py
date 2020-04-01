import yaml
import sqlalchemy
from etl.utils import xstr
from io import StringIO

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
    def __init__(self,config_yaml):
        self._config = config_yaml

    # Get credentials from yaml config
    def get_credentials(self, config_yaml, match_key):
        '''
        Returns an entry from yaml that matches match_key

        Arguments:
        config_yaml -- yaml object
        match_key -- string representing key to match in yaml dictionary

        '''
        credentials = dict()
        for key in config_yaml.keys():
            if key == match_key:
                return config_yaml[key]

    # Connect to remote mysql db
    def connect(self):
        # Get credentials from YAML
        credentials = self.get_credentials(self._config,'database_credentials')
        # create sqlalchemy engine
        engine = sqlalchemy.create_engine("{dialect}://{user}{pw}{host}/{db}"
                               .format(dialect = xstr(credentials['dialect_driver']),
                                        user = xstr(credentials['db_username']),
                                        host = ('@', '')[credentials['db_hostname'] is None] + xstr(credentials['db_hostname']),
                                        pw = (':', '')[credentials['db_password'] is None] + xstr(credentials['db_password']),
                                        db = xstr(credentials['db_name'])))
        # try to connect to database
        try:
            engine.connect()
            self._engine = engine
            self._metadata = sqlalchemy.MetaData()
        except sqlalchemy.exc.OperationalError as except_detail:
            # print error details
            print("ERROR: {}".format(except_detail))
        return

    def insert(self, df_dict):
        '''
        Arguments:
        df_dict -- dictionary containing transformed dataframes
        '''
        for tablename, df in df_dict.items():
            # If table don't exist, Create.
            if not self._engine.dialect.has_table(self._engine, tablename):
                print("Creating DB...")
                df.iloc[:0].to_sql(tablename, self._engine, if_exists='fail')

            df.to_sql(name=tablename, con = self._engine, if_exists = 'replace', chunksize = 1000)


    # def insert(self, dataframe, table_name, chunksize):
    #     '''
    #     Arguments:
    #     dataframe -- pandas dataframe object
    #     table_name -- table name in database
    #     chunksize -- writes records in batches of a given size at a time. By default, all rows will be written at once.
    #
    #     '''
    #     # Insert DataFrame recrds one by one.
    #     data.to_sql(attribute, con = self._engine, if_exists = 'append', chunksize = 1000)
    #     return
    #
    # def update(self, dataframe, table_name):
    #     '''
    #     Arguments:
    #     dataframe -- pandas dataframe object
    #     table_name -- table name in database
    #     '''
    #     # Update matching ids with dataframe in table_name
    #     pass
