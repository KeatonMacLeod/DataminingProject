<<<<<<< HEAD
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


=======

from sqlalchemy import create_engine
import pandas as pd
import pyfpgrowth
import numpy as np
import datetime
import timeit

import matplotlib.pyplot as plt
import numpy as np

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
        processedArray.append(subArray)
    return processedArray

def plotChart(xAxis, yAxis):
    print(xAxis)
    print(yAxis)
    plt.plot(xAxis, yAxis)
    plt.axis([1, 30, 0, 400])
    plt.title('About as simple as it gets, folks')
    plt.grid(True)
    # plt.savefig("JanTotal.png")
    plt.show()

def processByMonth(dictionary):
    xAxis = list(range(1, 32))
    yAxis = [0]*31
    day = ''
    for a in dictionary:
        if len(a) == 3:
            print(a)
            try:
                if '-' in a[0]:
                    print(day)
                    day = int(a[0].split('-')[2].split()[0])
                else: 
                    print(day)
                    day = int(a[1].split('-')[2].split()[0])
                delayValue = int(dictionary[a])
                if yAxis[day-1]>0:
                    yAxis[day-1] = yAxis[day-1]+delayValue
                else:
                    yAxis[day-1] = delayValue
            except:
                print('Skipping data point due to parsing error')
    print("xAxis:"+str(xAxis))
    print("yAxis:"+str(yAxis))
    plotChart(xAxis,yAxis)
    return None

sql_query = "SELECT T.Time, T.report_date, T.Incident FROM delays_merged as T WHERE T.month='Jan'"
db_connection = "mysql+pymysql://root@localhost/bus_delays"

conn = create_engine(db_connection)
df = pd.read_sql(sql_query, conn)
df = df.replace(to_replace='None', value="").dropna()
direction = df.values
direction = processTimeDeltaObjs(direction)
patterns = pyfpgrowth.find_frequent_patterns(direction, 2)
processByMonth(patterns)



#Timer functionalities

# start = timeit.default_timer()
# print('Printing:.....')
# stop = timeit.default_timer()
# print('Database Reading Time: ', stop - start) 

# start = timeit.default_timer()
# Removing all the elemnts of type None
# stop = timeit.default_timer()
# print('Replace  Time: ', stop - start)


# start = timeit.default_timer()
# stop = timeit.default_timer()
# print('Pattern Finding  Time: ', stop - start)
