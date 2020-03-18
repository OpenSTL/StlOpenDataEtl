from etl.transformer.vacant_table import fields

# vacantMapping dictionary format:
#   'column in final table': 'column data source'
# source can be:
# - the literal string 'TODO' (not implemented)
# - a string, meaning a direct map from another column
# - a function which iterates over every row, receives the existing row as a parameter, and returns the value for the column in the final table
vacantMapping = {
    'Handle': 'Handle',
    'Nbrhd': 'Nbrhd',
    'NHD_NAME': fields.neighborhoodName,
    'SITEADDR': 'SITEADDR',
    'ZIP': 'ZIP',
    'VacCatText': 'TODO',   # this is another "code to text" translation field, but the vocabulary isn't on the stl website.
    'ResFullBat': 'FullBaths',
    'ResHlfBath': 'HalfBaths',
    'Bath_Total': fields.calculateBathTotal,
    'ComGrdFlr': 'GroundFloorArea',
    'ResStories': fields.resStories,
    'Ward10': 'Ward10',
    'ResAttic': 'Attic',
    'ResBsmt': fields.resBsmt,
    'ResExtWall': fields.resExtWall,
    'ComConst': fields.comConst,
    'ResOccType': fields.resOccType,
    'ResGarage': fields.garageTotal,
    'ResCH': 'CentralHeating',
    'ResBmFin': fields.resBsmtFinishType,
    'SqFt': fields.calculateSqFt, # numeric value of total house square footage
    'Acres': fields.calculateAcres, # lot size
    # TODO: VB_decimal, NC_decimal, SL_decimal are computed in DB and rely on other fields (VB, NC, SL) that we will need to set
    'ResSalePri': 'ResSalePri',
    'Parcel': 'Parcel',
    'ParcelId': 'ParcelId'
}

# map raw STL fields to the fields used by the vacancy app
# mapping occurs in place
# fullyMergedPrcl = output of merging Prcl with other parcel related dataframes
def mapRawFieldsToVacantTableFields(fullyMergedPrcl):
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
