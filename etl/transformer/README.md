transform_task_vacant is a prototype for aggregating the vacancy app table from stl data sources.

this script is not integrated with the main program yet. I envision us allowing for multiple transform-and-load "tasks" like "vacancy table" and "mapbox layer".

to run, change `filename` and run transform_task_vacant.py by itself. I used a modified prcl.mdb with only the three important tables; otherwise loading takes even longer.