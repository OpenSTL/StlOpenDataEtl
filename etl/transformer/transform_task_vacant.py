import pandas_access as mdb
from parcel_id import parcelId

def transform_task_vacant():
    filename = '/Users/davidwilcox/src/bldgcom-bldgres-prcl.mdb'
    tables = [ 'BldgCom', 'BldgRes', 'Prcl']

    def loadTable(tableName):
        return mdb.read_table(filename, tableName)

    print('load tables')
    df = {}
    for tableName in tables:
        print(tableName)
        df[tableName] = loadTable(tableName)

    def keepOnlySelectColumnsInDf(df, columnsToKeep):
        df.drop(df.columns.difference(columnsToKeep), axis=1, inplace=True)

    print('drop unnnecessary Prcl columns')
    columnsWeWantFromPrcl = [
        'Handle',
        'Nbrhd',
        'Parcel',
        'ParcelId',
        'ResSalePri',
        'Ward10',
        'ZIP',
    ]
    keepOnlySelectColumnsInDf(df['Prcl'], columnsWeWantFromPrcl)

    print('add ParcelId to bldgcom, bldgres')
    def getParcelIdForRow(row):
        return parcelId(
            float(row['CityBlock']),
            int(row['Parcel']),
            int(row['OwnerCode'])
        )

    def addParcelIdColumn(df):
        df['ParcelId'] = df.apply(getParcelIdForRow, axis=1)

    addParcelIdColumn(df['BldgCom'])
    addParcelIdColumn(df['BldgRes'])

    print('drop unnecessary BldgCom columns')
    columnsWeWantFromBldgCom = [
        'ComGrdFlr',
        'ComConst',
        'ParcelId'
    ]
    keepOnlySelectColumnsInDf(df['BldgCom'], columnsWeWantFromBldgCom)

    print('drop unnecessary BldgRes columns')
    columnsWeWantFromBldgRes = [
        'ParcelId',
        'ResFullBat',
        'ResHlfBath',
        'ResStories',
        'ResAttic',
        'ResBsmt',
        'ResExtWall',
        'ResOccType',
        'ResGarage',
        'ResCH',
        'ResBmFin'
    ]
    keepOnlySelectColumnsInDf(df['BldgRes'], columnsWeWantFromBldgRes)

    print(df['Prcl'])
    
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

    print(fullyMergedPrcl)
    print('done')

if __name__ == "__main__":
    print('transform_task_vacant.py')
    transform_task_vacant()