import json

def stlVocabFileToDict(filename):
  with open(filename) as f:
    vocabFile = json.load(f)

  identifierToText = {}
  for vocabElement in vocabFile:
    identifierToText[vocabElement['IDENTIFIER']] = vocabElement['TITLE']

  return identifierToText

def getStlVocab():
    stlVocabFiles = [
      'commercial-building-construction-type',
      'residential-building-basement-finish-type',
      'residential-building-basement-type',
      'residential-building-exterior-wall-type',
      'residential-building-occupancy-type',
      'residential-building-stories-code',
      'neighborhood'
    ]

    stlVocab = {}
    for fileId in stlVocabFiles:
      stlVocab[fileId] = stlVocabFileToDict('./data/' + fileId + '.json')

    return stlVocab
