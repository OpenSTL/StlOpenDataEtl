from vocabulary import getVocabularyDictionaries

vocabData = getVocabularyDictionaries()

def calculateKmSq():
    # Convert geospatial area to km^2
    try:
        return float(row['geometry'].area/ 10**6)
    except: # nan
        return 0

def calculateSqFt():
    # Constant
    KM_PER_FT = 3280.84
    # Convert km^2 to sqft
    try:
        return float(calculateKmSq()*(KM_PER_FT**2))
    except: # nan
        return 0

def calculateAcres():
    # Constant
    SQFT_PER_ACRE = 43560
    # Convert sqft to acre
    try:
        return float(calculateSqFt()/(SQFT_PER_ACRE))
    except: # nan
        return 0

def calculateBathTotal(row):
    try:
        return float(row['FullBaths']) + 0.5 * float(row['HalfBaths'])
    except: # nan
        return 0

def comConst(row):
  return vocabData['commercial-building-construction-type'].get(
    row['ComConstType'],
    0,
  )

def garageTotal(row):
  garages = 0
  if (row['Garage1'] == '1'):
    garages += 1
  if (row['Garage2'] == '1'):
    garages += 1
  return garages

def neighborhoodName(row):
  return vocabData['neighborhood'].get(
    row['Nbrhd'],
    0
  )

def resBsmt(row):
  return vocabData['residential-building-basement-type'].get(
    row['BsmtType'],
    0
  )

def resBsmtFinishType(row):
  return vocabData['residential-building-basement-finish-type'].get(
    row['BsmtFinishType'],
    0
  )

def resExtWall(row):
  return vocabData['residential-building-exterior-wall-type'].get(
    row['ResExtWallType'],
    0
  )

def resOccType(row):
  return vocabData['residential-building-occupancy-type'].get(
    row['ResOccType'],
    0
  )

def resStories(row):
  return vocabData['residential-building-stories-code'].get(
    row['ResStoriesCode'],
    0
  )
