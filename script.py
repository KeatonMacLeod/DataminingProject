
from sqlalchemy import create_engine
import pandas as pd
import pyfpgrowth
import numpy as np
import datetime
import timeit

import matplotlib.pyplot as plt
import numpy as np

def prettyPrintDict(dictonary):
    count = 0
    for a in dictonary:
        if len(a) == 3:
            count +=1
            print(str(a) +':' + str(dictonary[a]))
    print(count)

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
    # plt.plot([1,27,28,29,5],[3,10,23,16,3])
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
start = timeit.default_timer()
df = pd.read_sql(sql_query, conn)
print('Printing:.....')
stop = timeit.default_timer()
print('Database Reading Time: ', stop - start) 

start = timeit.default_timer()
# Removing all the elemnts of type None
df = df.replace(to_replace='None', value="").dropna()
stop = timeit.default_timer()
print('Replace  Time: ', stop - start)

direction = df.values
direction = processTimeDeltaObjs(direction)

start = timeit.default_timer()
patterns = pyfpgrowth.find_frequent_patterns(direction, 2)
stop = timeit.default_timer()
print('Pattern Finding  Time: ', stop - start)
processByMonth(patterns)