from .map_parcel_data_to_vacant_table import map_parcel_data_to_vacant_table, vacant_table_fields
from .merge_parcel_data import merge_parcel_data
from .vacant_building_filter import vacant_building_filter

def keep_only_select_columns_in_df(df, columnsToKeep):
    df.drop(df.columns.difference(columnsToKeep), axis=1, inplace=True)

def vacant_table(all_tables):
    '''
    Transform extractor output into a dataframe that resembles the 
    "vacant" table

    Arguments:
    all_tables -- dictionary of dataframes from extractor

    Returns:
    Dataframe containing a list of records to be uploaded to
    the vacant app table.

    '''
    print('starting transformer vacant_table')

    # prune non-vacant parcels from the full parcel list
    vacant_parcels = vacant_building_filter(all_tables)

    # merge parcel data from multiple dataframes into a single dataframe
    # with a "one row = one parcel" format
    merged_parcel_data = merge_parcel_data(vacant_parcels, all_tables)

    # map parcel data into fields used by our Vacancy table
    map_parcel_data_to_vacant_table(merged_parcel_data)

    # remove fields we don't need
    print('prune unneeded fields')
    keep_only_select_columns_in_df(merged_parcel_data, vacant_table_fields.keys())
    
    print(merged_parcel_data)

    merged_parcel_data.to_csv('transform_vacant_table.csv', index=False)

    return merged_parcel_data
