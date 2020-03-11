import json

def stlVocabFileToDict(filename):
  with open(filename) as f:
    vocabFile = json.load(f)

  identifierToText = {}
  for vocabElement in vocabFile:
    identifierToText[vocabElement['IDENTIFIER']] = vocabElement['TITLE']

  return identifierToText

bsmtDict = stlVocabFileToDict('./data/residential-building-basement-type.json')
comConstDict = stlVocabFileToDict('./data/commercial-building-construction-type.json')
extWallDict = stlVocabFileToDict('./data/residential-building-exterior-wall-type.json')
storiesDict = stlVocabFileToDict('./data/residential-building-stories-code.json')

def comConst(row):
  return comConstDict.get(
    row['ComConst'],
    0,
  )

def resBsmt(row):
  return bsmtDict.get(
    row['BsmtType'],
    0
  )

def resExtWall(row):
  return extWallDict.get(
    row['ResExtWallType'],
    0
  )

def resStories(row):
  return storiesDict.get(
    row['ResStoriesCode'],
    0
  )
