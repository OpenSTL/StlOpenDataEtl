from .parcel_id import parcelId

def mergeParcelDataIntoSingleDataframe(df):
    '''
    Merge Parcel-related dataframes into a single dataframe using parcel id as a merge index.
    Only merges the dataframes we need to construct the "vacant" table
    Returns the merged dataframe

    Arguments:
    df -- dictionary of dataframes from extractor
    '''

    # add ParcelId as a merge index
    print('add ParcelId to bldgcom, bldgres')
    addParcelIdColumnToDf(df['BldgCom'])
    addParcelIdColumnToDf(df['BldgRes'])

    print('merge tables with prcl')
    print('-BldgCom')
    prclWithBldgCom = df['Prcl'].merge(
        right=df['BldgCom'],
        how='left',
        on='ParcelId'
    )

    print('-BldgRes')
    fullyMergedPrcl = prclWithBldgCom.merge(
        right=df['BldgRes'],
        how='left',
        on='ParcelId'
    )

    print('-par.dbf')
    fullyMergedPrcl = fullyMergedPrcl.merge(
        right=df['par.dbf'],
        how='left',
        left_on='Handle',
        right_on='HANDLE'
    )

    print('-prcl.shp')
    fullyMergedPrcl = fullyMergedPrcl.merge(
        right=df['prcl.shp'],
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
