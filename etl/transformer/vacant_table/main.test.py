# test harness for transform_vacant_table
# use only for testing the transformer outside of the main ETL app

from dbfread import DBF as DBFobj
from main import transform_vacant_table
import pandas as pd
import pandas_access as mdb

filenameParDbf = '/Users/davidwilcox/src/vacancydata/par.dbf'
filenamePrclMdb = '/Users/davidwilcox/src/bldgcom-bldgres-prcl.mdb'

def loadRelevantTables():
    print('load tables')

    # vacancy app depends on:
    # par.dbf
    # prcl.mdb: BldgCom, BldgRes, Prcl
    relevantAccessTables = [ 'BldgCom', 'BldgRes', 'Prcl' ]

    df = {}
    for tableName in relevantAccessTables:
        print('-- loading table %s' % tableName)
        df[tableName] = loadAccessTable(tableName)
    
    print('-- loading par.dbf')
    df['par.dbf'] = loadDbf(filenameParDbf)

    return df

def loadAccessTable(tableName):
    return mdb.read_table(filenamePrclMdb, tableName)

def loadDbf(filename):
    # Create DBF object
    table = DBFobj(filename)
    # Convert DBF object to dataframe
    dataframe = pd.DataFrame(iter(table))
    return dataframe

if __name__ == "__main__":
    print('testing transformer/vacant_table')
    df = loadRelevantTables()
    transform_vacant_table(df)
