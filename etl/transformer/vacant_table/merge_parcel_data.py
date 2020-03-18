from etl.transformer.vacant_table.parcel_id import parcelId

# df = dictionary of dataframes from extractor
# merge Parcel-related dataframes into a single dataframe using parcel id as a merge index
# only merges the dataframes we need to construct the "vacant" table
# returns the merged dataframe
def mergeParcelDataIntoSingleDataframe(df):
    # add ParcelId as a merge index
    print('add ParcelId to bldgcom, bldgres')
    addParcelIdColumnToDf(df['BldgCom'])
    addParcelIdColumnToDf(df['BldgRes'])

    print('merge BldgCom fields with prcl')
    prclWithBldgCom = df['Prcl'].merge(
        right=df['BldgCom'],
        how='left',
        on='ParcelId'
    )

    print('merge BldgRes fields with prcl')
    fullyMergedPrcl = prclWithBldgCom.merge(
        right=df['BldgRes'],
        how='left',
        on='ParcelId'
    )

    print('merge par.dbf with prcl')
    fullyMergedPrcl = fullyMergedPrcl.merge(
        right=df['par.dbf'],
        how='left',
        left_on='Handle',
        right_on='HANDLE'
    )

    return fullyMergedPrcl

def addParcelIdColumnToDf(df):
    df['ParcelId'] = df.apply(getParcelIdForRow, axis=1)

def getParcelIdForRow(row):
    return parcelId(
        float(row['CityBlock']),
        int(row['Parcel']),
        int(row['OwnerCode'])
    )

