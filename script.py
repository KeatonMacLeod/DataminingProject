# 
# import pyfpgrowth
# transactions = [[1, 2, 5],
#                 [2, 4],
#                 [2, 3],
#                 [1, 2, 4],
#                 [1, 3],
#                 [2, 3],
#                 [1, 3],
#                 [1, 2, 3, 5],
#                 [1, 2, 3]]
# patterns = pyfpgrowth.find_frequent_patterns(transactions, 2)
# rules = pyfpgrowth.generate_association_rules(patterns, 0.7)
# print("Patterns: "+str(patterns))
# print("Rules: "+str(rules))
#


#SQL STATEMENT FOR CORRELATIN: Incident, Direction, Location: Select L.Location, I.Incident, D.Direction FROM location as L LEFT JOIN incident as I ON I.delay_id = L.delay_id LEFT JOIN direction as D ON D.delay_id = L.delay_id LIMIT 5000
#SQL STATEMENT FOR CORRELATIN: Incident, Direction, Time: Select T.Time, I.Incident, D.Direction FROM delay as T LEFT JOIN incident as I ON I.delay_id = T.id LEFT JOIN direction as D ON D.delay_id = T.id LIMIT 5000
#SQL STATEMENT FOR CORRELATIN: Time, Report Date, Incident: Select T.Time, T.`Report Date`, I.Incident FROM delay as T LEFT JOIN incident as I ON I.delay_id = T.id LIMIT 20000



from sqlalchemy import create_engine
import pandas as pd
import pyfpgrowth
import numpy as np
import datetime

def prettyPrintDict(dictonary):
    for a in dictonary:
        if len(a) == 3:
            print(str(a) +':' + str(dictonary[a]))

#assuming the first element is the time delta object
def processTimeDeltaObjs(array):
    processedArray = []
    for el in array:
        subArray = []
        for e in el:
            if isinstance(e, str):
                subArray.append(e)
            elif isinstance(e,datetime.datetime):
                subArray.append(str(e))
            elif isinstance(e,datetime.timedelta):
                subArray.append(str(e.total_seconds()))
            # subArray.append(str(e))
        processedArray.append(subArray)
    return processedArray

db_connection = "mysql+pymysql://root@localhost/bus_delays"

conn = create_engine(db_connection)

df = pd.read_sql("Select T.Time, T.`Report Date`, I.Incident FROM delay as T LEFT JOIN incident as I ON I.delay_id = T.id LIMIT 10000", conn)

# Removing all the elemnts of type None
df = df.replace(to_replace='None', value="").dropna()

# print(df['direction'].values)

# direction = df['direction'].values
direction = df.values
direction = processTimeDeltaObjs(direction)

# print(direction)
patterns = pyfpgrowth.find_frequent_patterns(direction, 7)
# print(patterns)
# rules = pyfpgrowth.generate_association_rules(patterns, 0.7)
# print("Rules: "+str(rules))

prettyPrintDict(patterns)


