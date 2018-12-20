# COMP 4710 Group Project
# Fall 2018

Our cleaned data can be obtained by running BusDelayDatabase.sql in MySQL then running our cleaning scripts.
TODO Do we need to import location and stop tables separately

Note: all scripts assume you are running them from the directory they are located in
(name of schema might need to be changed in some cleaning scripts to match that when establishing the  database connection)

$ python3 directionTableCleaning.py
$ python3 InsertTTCStops.py *****TODO did we need to insert stops first? how?
$ python3 LocationNameConsistencyDifflib.py
$ python3 Merge_Tables.py
(delays merged.csv corresponds to this intermediate state)
$ python3 binning.py

Count Tables:
$ python3 Counts_Tables/pattern_counts_per_year.py
For the venn diagram:
$ R Counts_Tables/4_way_venn.R

Bar Charts:
$ python3 Bar_Charts/predictDelayByDayAndTime.py

Prediction:
$ python3 predictive_model.py //TODO add folder?

Decision Tree:
$ python3 Decision_Tree/regression_analysis.py


Information about the meaning of columns:
TTC_Bus_Delay_Data_Metadata.xlsx
