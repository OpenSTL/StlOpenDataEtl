from .map_fields import mapRawFieldsToVacantTableFields, vacantMapping
from .merge_parcel_data import mergeParcelDataIntoSingleDataframe

def keepOnlySelectColumnsInDf(df, columnsToKeep):
    df.drop(df.columns.difference(columnsToKeep), axis=1, inplace=True)

def transform_vacant_table(df):
    '''
    Transform extractor output into dataframe that resembles the 
    "vacant" table

    Arguments:
    df -- dictionary of dataframes from extractor

    '''
    print('starting transformer vacant_table')

    # merge parcel data into a single dataframe with a "one row = one parcel" format
    mergedParcelData = mergeParcelDataIntoSingleDataframe(df)

    # map raw fields into fields our Vacancy table uses
    mapRawFieldsToVacantTableFields(mergedParcelData)

    # remove fields we don't need
    print('prune unneeded fields')
    keepOnlySelectColumnsInDf(mergedParcelData, vacantMapping.keys())
    
    print(mergedParcelData)

    mergedParcelData.to_csv('merged.csv', index=False)
