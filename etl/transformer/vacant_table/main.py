from map_fields import mapRawFieldsToVacantTableFields, vacantMapping
from merge_parcel_data import mergeParcelDataIntoSingleDataframe

def keepOnlySelectColumnsInDf(df, columnsToKeep):
    df.drop(df.columns.difference(columnsToKeep), axis=1, inplace=True)

# transform extractor output into dataframe resembling "vacant" table
# df = dictionary of dataframes from extractor
def transform_vacant_table(df):

    # merge parcel data into a single dataframe with a "one row = one parcel" format
    fullyMergedPrcl = mergeParcelDataIntoSingleDataframe(df)

    # map raw fields into fields our Vacancy table uses
    mapRawFieldsToVacantTableFields(fullyMergedPrcl)

    # remove fields we don't need
    print('prune unneeded fields')
    keepOnlySelectColumnsInDf(fullyMergedPrcl, vacantMapping.keys())
    
    print(fullyMergedPrcl)
    print('done')

    fullyMergedPrcl.to_csv('merged.csv', index=False)
