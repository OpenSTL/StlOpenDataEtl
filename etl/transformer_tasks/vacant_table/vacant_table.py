from .map_parcel_data_to_vacant_table import map_parcel_data_to_vacant_table, vacant_table_fields
from .merge_parcel_data import merge_parcel_data
import logging
from etl import utils

def keep_only_select_columns_in_df(df, columnsToKeep):
    df.drop(df.columns.difference(columnsToKeep), axis=1, inplace=True)

def vacant_table(df):
    '''
    Transform extractor output into a dataframe that resembles the
    "vacant" table

    Arguments:
    df -- dictionary of dataframes from extractor

    Returns:
    Dataframe containing a list of records to be uploaded to
    the vacant app table.

    '''
    logging.debug('starting transformer vacant_table')

    # merge parcel data into a single dataframe with a "one row = one parcel" format
    merged_parcel_data = merge_parcel_data(df)

    # map parcel data into fields used by our Vacancy table
    map_parcel_data_to_vacant_table(merged_parcel_data)

    # remove fields we don't need
    logging.debug('prune unneeded fields')
    keep_only_select_columns_in_df(merged_parcel_data, vacant_table_fields.keys())

    logging.debug(merged_parcel_data)

    utils.to_csv(merged_parcel_data, 'transform_vacant_table.csv')

    return merged_parcel_data
