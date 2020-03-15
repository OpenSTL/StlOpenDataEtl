import 'stl-vocab-files' as StlVocabFiles

stlVocab = StlVocabFiles.getStlVocab()

def calculateBathTotal(row):
    try:
        return float(row['FullBaths']) + 0.5 * float(row['HalfBaths'])
    except: # nan
        return 0

def comConst(row):
  return stlVocab['commercial-building-construction-type'].get(
    row['ComConst'],
    0,
  )

def garageTotal(row):
  garages = 0
  if (row['Garage1']) garages += 1
  if (row['Garage2']) garages += 1
  return garages

def neighborhoodName(row):
  return stlVocab['neighborhood'].get(
    row['Nbrhd'],
    0
  )

def resBsmt(row):
  return stlVocab['residential-building-basement-type'].get(
    row['BsmtType'],
    0
  )

def resBsmtFinishType(row):
  return stlVocab['residential-building-basement-finish-type'].get(
    row['BsmtFinishType'],
    0
  )

def resExtWall(row):
  return stlVocab['residential-building-exterior-wall-type'].get(
    row['ResExtWallType'],
    0
  )

def resOccType(row):
  return stlVocab['residential-building-occupancy-type'].get(
    row['ResOccType'],
    0
  )

def resStories(row):
  return stlVocab['residential-building-stories-code'].get(
    row['ResStoriesCode'],
    0
  )
