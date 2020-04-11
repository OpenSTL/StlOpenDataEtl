#!/usr/bin/env python3

import csv
import geopandas
import os
import pandas as pd
import pandas_access
import shutil
from etl import utils
from dbfread import DBF as DBFobj
from io import StringIO
# from etl.entity import Entity
import logging
from etl import utils
from etl.constants import *


class Extractor:
    '''
    A class to extract data from various formats (CSV, MDB, DFB, SHP) into pandas dataframe

    '''

    # Initializer / Instance Attributes
    def __init__(self, pbar_manager):
        self.pbar_manager = pbar_manager
        self.job_count = 0
        self.logger = logging.getLogger(__name__)

    def extract_all(self, responses):
        '''
        Returns a list of extracted data objects from responses
        '''
        # Setup progress bar
        self.job_count = sum(map(lambda response: response.payload_count(), responses))
        # self.logger.debug("self.job_count: %s",self.job_count)
        self.pbar = self.pbar_manager.counter(total=self.job_count, desc=__name__, unit='files')

        # Prepare nested dictionary to store extracted table lists
        entity_dict = dict()
        entities = dict()
        for response in responses:
            for payload in response.payload:
                # Extract payload
                if utils.get_file_ext(payload.filename) == CSV:
                    entities = self.get_csv_data(payload)
                elif utils.get_file_ext(payload.filename) == MDB:
                    entities = self.get_mdb_data(payload)
                elif utils.get_file_ext(payload.filename) == DBF:
                    entities = self.get_dbf_data(payload)
                elif utils.get_file_ext(payload.filename) == SHP:
                    entities = self.get_shp_data(response, payload)
                else:
                    entities = {}
                # Add to master entity list
                entity_dict.update(entities)
                # update progress bar
                self.pbar.update()
        # close progress bar
        self.pbar.close()
        return entity_dict

    # Get attributes that matches user-specified data type
    def get_attributes_by_data_type(self, db_schema, data_type):
        '''
        Returns Entity object (str,dataframe) from a successful extraction

        Arguments:
        payload -- payload object (str,binary)
        '''
        # Declare dictionary to store matching attributes
        matching_attribute_dict = dict()
        # For each table, find attributes that are of specified data type
        for table_name in db_schema:
            matching_attributes = [key for key,value in db_schema[table_name].items() if value == data_type]
            matching_attribute_dict[table_name] = matching_attributes
        return matching_attribute_dict

    # Extract .csv data
    def get_csv_data(self, payload):
        '''
        Returns list of Entity object(s) (str,dataframe) from a successful extraction

        Arguments:
        payload -- payload object (str,binary)
        '''
        self.logger.debug('Extracting file: %s...', payload.filename)
        # Convert ByteIO to string
        content = str(payload.data.getvalue(),'utf-8')
        # Feed string as StringIO into pandas read_csv function()
        dataframe = pd.read_csv(StringIO(content))
        # Use HANDLE as dataframe index
        # dataframe.set_index('HANDLE', inplace = True)
        # Return Entity object
        return {payload.filename: dataframe}

    # Extract .mdb data
    def get_mdb_data(self, payload):
        '''
        Returns list of Entity object(s) (str,dataframe) from a successful extraction

        Arguments:
        payload -- payload object (str,binary)
        '''
        # TODO: find a way to directly pass byteio to reading utility without writing to disk
        try:
            # Write to bytes to disk
            open(payload.filename, 'wb').write(payload.data.getvalue())

            # Get database schema
            mdb_schema = pandas_access.read_schema(payload.filename)
            # Get attributes that are of integer type
            integer_attributes = self.get_attributes_by_data_type(mdb_schema,'Long Integer')

            # Declare entity dict
            entity_dict = dict()

        	# Get list of table from database
        	table_list = pandas_access.list_tables(payload.filename)

        	# Update progress bar job count
        	self.job_count += len(table_list)
        	self.pbar.total = self.job_count

            # Iterate through each table in database
        	for tbl in table_list:
                self.logger.debug('Extracting table: \'%s\' from file: %s...', tbl, payload.filename)
                # Issue: Default pandas integer type is not nullable - null values in integer column causes read error
                # Workaround: Read integer as Int64 (pandas nullable integer type in pandas)
                dtype_input = {attribute:'Int64' for attribute in integer_attributes[tbl]}
                df = pandas_access.read_table(payload.filename, tbl, dtype = dtype_input)
                entity_dict.update({tbl:df})
                # update progress bar
                self.pbar.update()
            return entity_dict
        finally:
            utils.silentremove(payload.filename)

    # Extract .dbf data
    def get_dbf_data(self, payload):
        '''
        Returns list of Entity object(s) (str,dataframe) from a successful extraction

        Arguments:
        payload -- payload object (str,binary)
        '''
        self.logger.debug('Extracting file: %s...', payload.filename)
        # TODO: find a way to directly pass byteio into DBFobj without writing to disk
        try:
            # Write bytes to disk
            open(payload.filename, 'wb').write(payload.data.getvalue())
            # Create DBF object
            table = DBFobj(payload.filename)
            # Convert DBF object to dataframe
            dataframe = pd.DataFrame(iter(table))
            # Use HANDLE as dataframe index
            # dataframe.set_index('HANDLE', inplace = True)
            # Return Entity object
            return {payload.filename: dataframe}
        finally:
            utils.silentremove(payload.filename)

    # Extract .shp data
    def get_shp_data(self, archive, shapefile):
        '''
        Returns list of Entity objects from a successful extraction

        Arguments:
        archive -- fetcher response from the archive containing the shape file and supporting files (FetcherResponse)
        shapefile -- the shape file from the archive; looks like the "payload" argument in other extractors
        '''
        self.logger.debug('Extracting file: %s...', shapefile.filename)
        SCRATCH_DIR = 'scratch'
        try:
            # .shp requires multiple supporting files; save all files from archive to disk
            os.mkdir(SCRATCH_DIR)
            for archivedFile in archive.payload:
                archivedFilename = os.path.join(SCRATCH_DIR, archivedFile.filename)
                open(archivedFilename, 'wb').write(archivedFile.data.getvalue())

            shapeFilename = os.path.join(SCRATCH_DIR, shapefile.filename)
            dataframe = geopandas.read_file(shapeFilename)
            return {shapefile.filename: dataframe}

        finally:
            shutil.rmtree(SCRATCH_DIR)
