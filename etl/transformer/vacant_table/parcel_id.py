def parcelId(cityBlock, parcel, ownerCode):
    return "%06d%04d%01d" % (cityBlock * 100, parcel, ownerCode)

if __name__ == "__main__":
    print('parcel_id.py')
    assert parcelId(1, 5, 0) == '00010000050'
    assert parcelId(14, 5, 0) == '00140000050'
    assert parcelId(127, 185, 0) == '01270001850'
    assert parcelId(6210, 100, 0) == '62100001000'
    assert parcelId(1365, 70, 7) == '13650000707'
    print('parcelId tests passed')
