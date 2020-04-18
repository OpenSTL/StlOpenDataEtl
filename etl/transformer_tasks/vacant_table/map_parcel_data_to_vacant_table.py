from . import fields
import logging

'''
vacant_table_fields is a dictionary representing every field in the "vacant" table
the key is a field name in the vacant table
the value explains how to derive the field from the raw parcel data
the value can be:
- the literal string 'TODO', representing a field that we don't know how to derive yet
- a string, meaning a direct map from another column
- a function which iterates over every row, receives the existing row as a parameter, and returns the value for the column in the final table
if you want a field in the final "vacant" table, make sure it is here. all other fields will be pruned.
'''
vacant_table_fields = {
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
    'VB_decimal': 'TODO',
    'NC_decimal': 'TODO',
    'SL_decimal': 'TODO',
    'ResSalePri': 'ResSalePri',
    'Parcel': 'Parcel',
    'ParcelId': 'ParcelId'
}

def map_parcel_data_to_vacant_table(merged_parcel_data):
    '''
    Map aggregated parcel data to the fields used by the vacancy app.
    Mapping occurs in-place.

    Arguments:
    merged_parcel_data -- output of merging Prcl with other parcel related dataframes
    '''

    logging.debug('map data to columns')
    for column, source in vacant_table_fields.items():
        if column == source:
            continue
        elif source == 'TODO':
            logging.debug('-- TODO implement field %s' % column)
        elif type(source) == str:
            merged_parcel_data[column] = merged_parcel_data[source]
        else:
            merged_parcel_data[column] = merged_parcel_data.apply(source, axis=1)
