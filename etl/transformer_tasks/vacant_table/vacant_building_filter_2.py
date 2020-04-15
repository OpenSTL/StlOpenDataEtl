import pandas as pd

def vacant_building_filter(df):
    prcl = df['Prcl']
    prcl['owned_by_lra_and_contains_building'] = prcl.apply(owned_by_lra_and_contains_building, axis = 1)
    prcl['marked_vacant_by_annual_survey_since_2017'] = prcl.apply(marked_vacant_by_annual_survey, axis = 1, args=(df))
    prcl['marked_vacant_by_forestry_dept'] = prcl.apply(marked_vacant_by_forestry_dept, axis = 1, args=(df))
    prcl['building_is_structurally_condemned'] = prcl.apply(building_is_structurally_condemned, axis = 1, args=(df))
    prcl['boarded_up_at_least_once_since_2017'] = prcl.apply(boarded_up_at_least_once_since_2017, axis = 1, args=(df))

def owned_by_lra_and_contains_building(df, parcel):
    return parcel['OwnerName'] == 'LRA' and (parcel['NbrOfBldgsRes'] + parcel['NbrOfBldgsCom']) > 0

def test_owned_by_lra_and_contains_building():
    p1 = pd.Series({'OwnerName': 'LRA', 'NbrOfBldgsRes': 1, 'NbrOfBldgsCom': 1})
    assert owned_by_lra_and_contains_building(None, p1)
    p2 = pd.Series({'OwnerName': 'definitely not LRA', 'NbrOfBldgsRes': 1, 'NbrOfBldgsCom': 1})
    assert owned_by_lra_and_contains_building(None, p2) == False
    p3 = pd.Series({'OwnerName': 'LRA', 'NbrOfBldgsRes': 0, 'NbrOfBldgsCom': 0})
    assert owned_by_lra_and_contains_building(None, p3) == False
        
# check through BldgInsp.VacBldg and find a matching Handle with SurveyYear >= 2017
def marked_vacant_by_annual_survey(df, parcel):
    vacbldg = df['VacBldg']
    try:
        return vacbldg.loc[[parcel.Handle]].query('SurveyYear >= 2017').size > 0
    except KeyError:
        return False

def test_marked_vacant_by_annual_survey():
    vacbldg = pd.DataFrame({'Handle': [1, 2], 'SurveyYear': [1988, 2017]})
    vacbldg.set_index('Handle', inplace=True)
    df = { 'VacBldg': vacbldg }

    results = [False, True, False]
    for handle in range(1, 3):
        parcel = pd.Series({'Handle': handle})
        assert marked_vacant_by_annual_survey(df, parcel) == results[handle - 1]

def marked_vacant_by_forestry_dept(df, parcel):
    forestry_maintenance_properties = df['forestry_maintenance_properties']
    try:
        return forestry_maintenance_properties.loc[[parcel.Handle]].query('PROPERTYTYPE == "VACANT BUILDING"').size > 0
    except KeyError:
        return False

def test_marked_vacant_by_forestry_dept():
    forestry = pd.DataFrame({'Handle': [1, 2], 'PROPERTYTYPE': ['something else', 'VACANT BUILDING']})
    forestry.set_index('Handle', inplace=True)
    df = { 'forestry_maintenance_properties': forestry }

    results = [False, True, False]
    for handle in range(1, 3):
        parcel = pd.Series({'Handle': handle})
        assert marked_vacant_by_forestry_dept(df, parcel) == results[handle - 1]


def building_is_structurally_condemned(df, parcel):
    condemn = df['Condemn']
    try:
        # if we find a match it's condemned, no need for addl conditions
        condemn.loc[parcel['Handle']]
        return True
    except KeyError:
        return False

def test_building_is_structurally_condemned():
    condemn = pd.DataFrame({'Handle': [1]})
    condemn.set_index('Handle', inplace=True)
    df = { 'Condemn': condemn }
    p1 = pd.Series({'Handle': 1})
    assert(building_is_structurally_condemned(df, p1) == True)
    p2 = pd.Series({'Handle': 2})
    assert(building_is_structurally_condemned(df, p2) == False)

# match parcel handle to public_inventory, then match inventory Record No to boardup_public
def boarded_up_at_least_once_since_2017(df, parcel):
    try:
        public_inventory = df['dbo_vw_public_inventory']
        boardup_public = df['dbo_vw_boardup_public']

        record_number = public_inventory.loc[parcel.Handle, 'Record_No']
        return boardup_public.loc[[record_number]].query('Completion_date >= 20170101').size > 0

    except KeyError:
        return False

def test_boarded_up_at_least_once_since_2017():
    public_inventory = pd.DataFrame({'Handle': [1, 2], 'Record_No': [1001, 2002]})
    public_inventory.set_index('Handle', inplace=True)
    boardup_public = pd.DataFrame({'Record_No': [1001, 2002], 'Completion_date': [pd.Timestamp('12/31/2016'), pd.Timestamp('1/1/2017')]})
    boardup_public.set_index('Record_No', inplace=True)
    df = {
        'dbo_vw_public_inventory': public_inventory,
        'dbo_vw_boardup_public': boardup_public,
    }
    results = [False, True, False]
    for handle in range(1, 3):
        parcel = pd.Series({'Handle': handle})
        assert(boarded_up_at_least_once_since_2017(df, parcel) == results[handle - 1])

if __name__ == "__main__":
    print('vacant_building_filter_2.py')

    test_owned_by_lra_and_contains_building()
    test_marked_vacant_by_annual_survey()
    test_marked_vacant_by_forestry_dept()
    test_building_is_structurally_condemned()
    test_boarded_up_at_least_once_since_2017()