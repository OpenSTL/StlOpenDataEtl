import os
import json

def vocabularyFileToDictionary(filename):
  with open(filename) as f:
    vocabFile = json.load(f)

  identifierToText = {}
  for vocabElement in vocabFile:
    identifierToText[vocabElement['IDENTIFIER']] = vocabElement['TITLE']

  return identifierToText

def getVocabularyDictionaries():
    vocabularyFiles = [
      'commercial-building-construction-type',
      'residential-building-basement-finish-type',
      'residential-building-basement-type',
      'residential-building-exterior-wall-type',
      'residential-building-occupancy-type',
      'residential-building-stories-code',
      'neighborhood'
    ]

    vocabDictionaries = {}
    for fileId in vocabularyFiles:
      relativeFilename = os.path.join('./vocabulary_data/' + fileId + '.json')
      curdir = os.path.dirname(os.path.abspath(__file__))
      filename = os.path.join(curdir, relativeFilename)
      vocabDictionaries[fileId] = vocabularyFileToDictionary(filename)

    return vocabDictionaries
