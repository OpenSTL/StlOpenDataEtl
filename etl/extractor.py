#!/usr/bin/env python3

import csv
import geopandas
import os
import pandas as pd
import pandas_access
import shutil
from dbfread import DBF as DBFobj
from io import StringIO
from etl.entity import Entity


class Extractor:
    '''
    A class to extract data from various formats (CSV, MDB, DFB, SHP) into pandas dataframe

    '''

    # Initializer / Instance Attributes
    def __init__(self):
        pass

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
        # Write to bytes to disk
        open(payload.filename, 'wb').write(payload.data.getvalue())

        # Get database schema
        mdb_schema = pandas_access.read_schema(payload.filename)
        # Get attributes that are of integer type
        integer_attributes = self.get_attributes_by_data_type(mdb_schema,'Long Integer')

        # Declare entity dict
        entity_dict = dict()
        # Iterate through each table in database
        for tbl in pandas_access.list_tables(payload.filename):
                # Issue: Default pandas integer type is not nullable - null values in integer column causes read error
                # Workaround: Read integer as Int64 (pandas nullable integer type in pandas)
                dtype_input = {attribute:'Int64' for attribute in integer_attributes[tbl]}
                df = pandas_access.read_table(payload.filename, tbl, dtype = dtype_input)
                entity_dict.update({tbl:df})
        return entity_dict

    # Extract .dbf data
    def get_dbf_data(self, payload):
        '''
        Returns list of Entity object(s) (str,dataframe) from a successful extraction

        Arguments:
        payload -- payload object (str,binary)
        '''
        # TODO: find a way to directly pass byteio into DBFobj without writing to disk
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

    # Extract .shp data
    def get_shp_data(self, archive, shapefile):
        '''
        Returns list of Entity objects from a successful extraction

        Arguments:
        archive -- fetcher response from the archive containing the shape file and supporting files (FetcherResponse)
        shapefile -- the shape file from the archive; looks like the "payload" argument in other extractors
        '''
        SCRATCH_DIR = 'scratch'
        try:
            # .shp requires multiple supporting files; save all files from archive to disk
            os.mkdir(SCRATCH_DIR)
            for archivedFile in archive.payload:
                archivedFilename = os.path.join(SCRATCH_DIR, archivedFile.filename)
                open(archivedFilename, 'wb').write(archivedFile.data.getvalue())

            shapeFilename = os.path.join(SCRATCH_DIR, shapefile.filename)
            dataframe = geopandas.read_file(shapeFilename)
            # define geospatial projection
            dataframe.crs = {'init': 'epsg:32633'}
            return {shapefile.filename: dataframe}

        finally:
            shutil.rmtree(SCRATCH_DIR)
