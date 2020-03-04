import math
import pandas_access as mdb
from parcel_id import parcelId

filename = '/Users/davidwilcox/src/bldgcom-bldgres-prcl.mdb'

def addParcelIdColumnToDf(df):
    df['ParcelId'] = df.apply(getParcelIdForRow, axis=1)

def getParcelIdForRow(row):
    return parcelId(
        float(row['CityBlock']),
        int(row['Parcel']),
        int(row['OwnerCode'])
    )

def keepOnlySelectColumnsInDf(df, columnsToKeep):
    df.drop(df.columns.difference(columnsToKeep), axis=1, inplace=True)

def loadTable(tableName):
    return mdb.read_table(filename, tableName)

def calculateBathTotal(row):
    try:
        return float(row['FullBaths']) + 0.5 * float(row['HalfBaths'])
    except: # nan
        return 0

def transform_task_vacant():
    # vacancy app table is dependent on these three tables from prcl.mdb
    # we will merge the tables together based on ParcelId and then calculate
    # the fields we want in the final product
    tables = [ 'BldgCom', 'BldgRes', 'Prcl' ]

    # dictionary format:
    #   'column in final table': 'column data source'
    # source can be:
    # - the literal string 'TODO' (not implemented)
    # - a string, meaning a direct map from another column
    # - a function which iterates over every row, receives the existing row as a parameter, and returns the value for the column in the final table
    vacantMapping = {
        'Handle': 'Handle',
        'Nbrhd': 'Nbrhd',
        'NHD_NAME': 'TODO',
        'SITEADDR': 'TODO',
        'ZIP': 'ZIP',
        'VacCatText': 'TODO', # All rows say Vacant Building
        'ResFullBat': 'FullBaths',
        'ResHlfBath': 'HalfBaths',
        'Bath_Total': calculateBathTotal,
        'ComGrdFlr': 'GroundFloorArea',
        # In prcl, this is a string with the number of stories for the building e.g. 2 stories
        'ResStories': 'TODO', # TODO: not sure how to extract from BldgRes. There is a 'ResStoriesCode' which we may have to translate. 
        'Ward10': 'Ward10',
        'ResAttic': 'Attic',
        # Either full basement, partial basement or 0
        'ResBsmt': 'TODO', # TODO: not sure how to extract from BldgRes. I spy a BsmtType and BsmtFinishType
        # Either brick or 0
        'ResExtWall': 'TODO', # TODO: translate ResExtWallType code
        # brick & wood, brick & steel, brick & concrete; I would include the full text as I think buyers would want to know if it's a concrete foundation
        'ComConst': 'TODO', # TODO: BldgCom has codes like BW; our db has full text like "Brick & Wood"; which do we want?
        'ResOccType': 'TODO', # TODO: translate ResOccType code # one family, two family, four family, 0
        'ResGarage': 'TODO', # 1 = present, 0 = none
        'ResCH': 'CentralHeating',
        # finished, unfinished, dirt floor, 0
        'ResBmFin': 'TODO', # TODO: another translation one
        'SqFt': 'TODO', # numeric value of total house square footage
        'Acres': 'TODO', # lot size
        # TODO: VB_decimal, NC_decimal, SL_decimal are computed in DB and rely on other fields (VB, NC, SL) that we will need to set
        'ResSalePri': 'ResSalePri',
        'Parcel': 'Parcel',
        'ParcelId': 'ParcelId'
    }

    print('load tables')
    df = {}
    for tableName in tables:
        print('-- loading table %s' % tableName)
        df[tableName] = loadTable(tableName)

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

    print('map data to columns')
    for column, source in vacantMapping.items():
        if column == source:
            continue
        elif source == 'TODO':
            print('-- TODO implement field %s' % column)
        elif type(source) == str:
            if column != source:
                fullyMergedPrcl[column] = fullyMergedPrcl[source]
        else:
            fullyMergedPrcl[column] = fullyMergedPrcl.apply(source, axis=1)

    # remove fields we don't need
    print('prune unneeded fields')
    keepOnlySelectColumnsInDf(fullyMergedPrcl, vacantMapping.keys())
    
    print(fullyMergedPrcl)
    print('done')

    fullyMergedPrcl.to_csv('merged.csv', index=False)

if __name__ == "__main__":
    print('transform_task_vacant.py')
    transform_task_vacant()
