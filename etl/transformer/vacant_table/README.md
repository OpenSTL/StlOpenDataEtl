transformer/vacant_table
---
> vacant_table transforms raw data into the "vacant" table used by the openstl vacancy app

This script is not integrated with the main program yet. I envision us allowing for multiple transform-and-load "tasks" like "vacancy table" and "mapbox layer".

This is not complete. Needs help completing all of the computed fields and with the "Known Issues" section.

# Run instructions
1. Setup venv and requirements per the root README
1. Download prcl.mdb or get a modified mdb with only the three important tables. See main.test.py for a list of the "important" tables. Using only the important tables will cut down processing time if you are testing the transformer by itself.
1. Download par.dbf
1. Change `filenameParDbf` and `filenamePrclMdb` in `main.test.py` to point to your downloaded database files from the previous steps.
1. Run `python etl/transformer/vacant_table/main.test.py`

# Known Issues
1. At least one property (parcel id 3840001700) has two entries in BldgCom for two BldgNum values. Right now that creates two elements in our ending table. What is the correct approach?
1. Tons of "data holes" - properties missing records in BldgRes, BldgCom or both.
1. Needs integration with the rest of the ETL
