def parcel_id(cityBlock, parcel, ownerCode):
    return "%06d%04d%01d" % (cityBlock * 100, parcel, ownerCode)

if __name__ == "__main__":
    print('parcel_id.py')
    assert parcel_id(1, 5, 0) == '00010000050'
    assert parcel_id(14, 5, 0) == '00140000050'
    assert parcel_id(127, 185, 0) == '01270001850'
    assert parcel_id(6210, 100, 0) == '62100001000'
    assert parcel_id(1365, 70, 7) == '13650000707'
    print('parcel_id tests passed')
