from .map_fields import mapRawFieldsToVacantTableFields, vacantMapping
from .merge_parcel_data import mergeParcelDataIntoSingleDataframe

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
    print('starting transformer vacant_table')

    # merge parcel data into a single dataframe with a "one row = one parcel" format
    mergedParcelData = mergeParcelDataIntoSingleDataframe(df)

    # map raw fields into fields our Vacancy table uses
    mapRawFieldsToVacantTableFields(mergedParcelData)

    # remove fields we don't need
    print('prune unneeded fields')
    keep_only_select_columns_in_df(mergedParcelData, vacantMapping.keys())
    
    print(mergedParcelData)

    mergedParcelData.to_csv('transform_vacant_table.csv', index=False)

    return mergedParcelData
