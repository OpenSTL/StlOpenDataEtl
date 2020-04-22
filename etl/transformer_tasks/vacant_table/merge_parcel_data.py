from .parcel_id import parcel_id

def merge_parcel_data(parcels, all_tables):
    '''
    Merge Parcel-related dataframes into a single dataframe using parcel id as a merge index.
    Only merges the dataframes we need to construct the "vacant" table
    Returns the merged dataframe

    Arguments:
    parcels -- Dataframe for prcl table
    all_tables -- All tables from the extractor. This also includes prcl, but use parcels argument instead since parcels will be modified beyond the raw extractor output.
    '''

    # add ParcelId as a merge index
    print('add ParcelId to bldgcom, bldgres')
    add_parcel_id_column_to_df(all_tables['BldgCom'])
    add_parcel_id_column_to_df(all_tables['BldgRes'])

    print('merge tables with prcl')
    print('-BldgCom')
    prclWithBldgCom = parcels.merge(
        right=all_tables['BldgCom'],
        how='left',
        on='ParcelId'
    )

    print('-BldgRes')
    fullyMergedPrcl = prclWithBldgCom.merge(
        right=all_tables['BldgRes'],
        how='left',
        on='ParcelId'
    )

    # convert pk to int so we can merge with other data sources
    fullyMergedPrcl['Handle'] = fullyMergedPrcl['Handle'].astype(int)

    print('-par.dbf')
    parDbf = all_tables['par.dbf'].copy(deep=True)
    # cast par.dbf pk to match prcl pk type
    parDbf['HANDLE'] = parDbf['HANDLE'].astype(int)
    fullyMergedPrcl = fullyMergedPrcl.merge(
        right=parDbf,
        how='left',
        left_on='Handle',
        right_on='HANDLE'
    )

    print('-prcl.shp')
    prclShp = all_tables['prcl.shp'].copy(deep=True)
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
