# test harness for transform_vacant_table
# use only for testing the transformer outside of the main ETL app

import pandas_access as mdb
from main import transform_vacant_table

filename = '/Users/davidwilcox/src/bldgcom-bldgres-prcl.mdb'

def loadRelevantTables():
    print('load tables')

    # vacancy app table is dependent on these three tables from prcl.mdb
    relevantTables = [ 'BldgCom', 'BldgRes', 'Prcl' ]

    df = {}
    for tableName in relevantTables:
        print('-- loading table %s' % tableName)
        df[tableName] = loadTable(tableName)
    return df

def loadTable(tableName):
    return mdb.read_table(filename, tableName)

if __name__ == "__main__":
    print('testing transformer/vacant_table')
    df = loadRelevantTables()
    transform_vacant_table(df)
