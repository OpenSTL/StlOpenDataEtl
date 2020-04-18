from .parcel_id import parcel_id
import logging

def merge_parcel_data(df):
    '''
    Merge Parcel-related dataframes into a single dataframe using parcel id as a merge index.
    Only merges the dataframes we need to construct the "vacant" table
    Returns the merged dataframe

    Arguments:
    df -- dictionary of dataframes from extractor
    '''

    # add ParcelId as a merge index
    logging.debug('add ParcelId to bldgcom, bldgres')
    add_parcel_id_column_to_df(df['BldgCom'])
    add_parcel_id_column_to_df(df['BldgRes'])

    logging.debug('merge tables with prcl')
    logging.debug('-BldgCom')
    prclWithBldgCom = df['Prcl'].merge(
        right=df['BldgCom'],
        how='left',
        on='ParcelId'
    )

    logging.debug('-BldgRes')
    fullyMergedPrcl = prclWithBldgCom.merge(
        right=df['BldgRes'],
        how='left',
        on='ParcelId'
    )

    # convert pk to int so we can merge with other data sources
    fullyMergedPrcl['Handle'] = fullyMergedPrcl['Handle'].astype(int)

    logging.debug('-par.dbf')
    parDbf = df['par.dbf'].copy(deep=True)
    # cast par.dbf pk to match prcl pk type
    parDbf['HANDLE'] = parDbf['HANDLE'].astype(int)
    fullyMergedPrcl = fullyMergedPrcl.merge(
        right=parDbf,
        how='left',
        left_on='Handle',
        right_on='HANDLE'
    )

    logging.debug('-prcl.shp')
    prclShp = df['prcl.shp'].copy(deep=True)
    # again we need to change the pk type to match prcl
    prclShp['HANDLE'] = prclShp['HANDLE'].astype(int)
    fullyMergedPrcl = fullyMergedPrcl.merge(
        right=prclShp,
        how='left',
        left_on='Handle',
        right_on='HANDLE'
    )

    return fullyMergedPrcl

def add_parcel_id_column_to_df(df):
    df['ParcelId'] = df.apply(get_parcel_id_for_row, axis=1)

def get_parcel_id_for_row(row):
    return parcel_id(
        float(row['CityBlock']),
        int(row['Parcel']),
        int(row['OwnerCode'])
    )
