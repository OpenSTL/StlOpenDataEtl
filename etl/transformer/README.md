transform_task_vacant
---
> transform_task_vacant is a prototype for the "T"; aggregating the vacancy app table from stl data sources.

This script is not integrated with the main program yet. I envision us allowing for multiple transform-and-load "tasks" like "vacancy table" and "mapbox layer".

This is not complete. Needs help completing all of the computed fields and with the "Known Issues" section.

# Run instructions
1. Download prcl.mdb or get a modified mdb with only the three important tables. latter will take much less processing time.
2. Change `filename` to point to your prcl.mdb
3. Run `python etl/transformer/transform_task_vacant.py`

# Known Issues
1. At least one property (parcel id 3840001700) has two entries in BldgCom for two BldgNum values. Right now that creates two elements in our ending table. What is the correct approach?
2. Tons of "data holes" - properties missing records in BldgRes, BldgCom or both.
3. Needs integration with the rest of the ETL
