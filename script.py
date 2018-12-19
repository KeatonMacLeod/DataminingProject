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

def plotChart(xAxis, yAxis, axis):
    color = 'red'
    for i in range(0, len(xAxis)):
        plt.plot(xAxis[i], yAxis[i], color)
        color = 'green'

    plt.axis(axis)
    plt.title('About as simple as it gets, folks')
    plt.grid(True)
    # plt.savefig("JanTotal.png")
    plt.show()

def processByYear(dictionary):
    xAxis = list(range(1, 13))
    yAxis = [0]*12
    month = ''
    for a in dictionary:
        if len(a) == 2:
            # print(a)
            try:
                if '-' in a[0]:
                    # print(day)
                    month = int(a[0].split('-')[1])
                else: 
                    # print(day)
                    month = int(a[1].split('-')[1])
                delayValue = int(dictionary[a])
                if yAxis[month-1]>0:
                    yAxis[month-1] = yAxis[month-1]+delayValue
                else:
                    yAxis[month-1] = delayValue
            except:
                print('Skipping data point due to parsing error')
    # print("xAxis:"+str(xAxis))
    # print("yAxis:"+str(yAxis))
    
    return xAxis, yAxis

def processByMonth(dictionary):
    xAxis = list(range(1, 32))
    yAxis = [0]*31
    day = ''
    for a in dictionary:
        if len(a) == 2:
            # print(a)
            try:
                if '-' in a[0]:
                    # print(day)
                    day = int(a[0].split('-')[2].split()[0])
                else: 
                    # print(day)
                    day = int(a[1].split('-')[2].split()[0])
                delayValue = int(dictionary[a])
                if yAxis[day-1]>0:
                    yAxis[day-1] = yAxis[day-1]+delayValue
                else:
                    yAxis[day-1] = delayValue
            except:
                print('Skipping data point due to parsing error')
    # print("xAxis:"+str(xAxis))
    # print("yAxis:"+str(yAxis))
    
    return xAxis, yAxis

def processByHour(dictionary):
    xAxis = list(range(0, 24))
    yAxis = [0]*24
    hour = ''
    for a in dictionary:
        # print(a)
        if len(a) == 3:
            try:
                if ' ' not in a[0]:
                    hour = int(a[0].split(':')[0])
                else: 
                    # print(day)
                    hour = int(a[1].split(':')[0])
                delayValue = int(dictionary[a])
                if yAxis[hour]>0:
                    yAxis[hour] = yAxis[hour]+delayValue
                else:
                    yAxis[hour] = delayValue
            except:
                print('Skipping data point due to parsing error')
    print("xAxis:"+str(xAxis))
    print("yAxis:"+str(yAxis))
    
    return xAxis, yAxis


def processByDayOfWeek(dictionary):
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday']
    xAxis = list(range(0, 7))
    yAxis = [0]*7
    day = ''
    for a in dictionary:
        # print(a)
        if len(a) == 1:
            try:
                day = days.index(a[0])
                # print(day)
                delayValue = int(dictionary[a])
                if yAxis[day]>0:
                    yAxis[day] = yAxis[day]+delayValue
                else:
                    yAxis[day] = delayValue
            except:
                print('Skipping data point due to parsing error')
    # print("xAxis:"+str(xAxis))
    # print("yAxis:"+str(yAxis))
    return xAxis, yAxis

def prettyPrinPatterns(pattern):
    for a in pattern:
        print(str(a)+':'+str(pattern[a]))
    return None

sql_query = "SELECT T.report_date, T.Incident FROM delays_binned as T WHERE T.month='Sept' AND T.year = 2014"
db_connection = "mysql+pymysql://root@localhost/bus_delays"

conn = create_engine(db_connection)
df = pd.read_sql(sql_query, conn)
patterns = pyfpgrowth.find_frequent_patterns(processTimeDeltaObjs(df.values), 5)

df1 = pd.read_sql("SELECT T.report_date, T.Incident FROM delays_binned as T WHERE T.year!=2017 AND T.year!=2018", conn)

patterns1 = pyfpgrowth.find_frequent_patterns(processTimeDeltaObjs(df1.values), 5)

df2 = pd.read_sql("SELECT T.report_date, T.Incident FROM delays_binned as T WHERE T.year = 2017", conn)
patterns2 = pyfpgrowth.find_frequent_patterns(processTimeDeltaObjs(df2.values), 5)

xAxis, yAxis = processByMonth(patterns)
xAxis1, yAxis1 = processByMonth(patterns1)

prettyPrinPatterns(patterns1)
print('----------')
prettyPrinPatterns(patterns2)

yAxis1 = [float(i)/3 for i in yAxis1]

xAxis2, yAxis2 = processByMonth(patterns2)

print("xAxis1 Averaged:"+str(xAxis1))
averagedInt = [int(i) for i in yAxis1]
print("yAxis1 Averaged:"+str(averagedInt))
print('----------')
print("xAxis2:"+str(xAxis2))
print("yAxis2:"+str(yAxis2))
# xAxis, yAxis = processByDay(patterns)
# xAxis1, yAxis1 = processByDay(patterns1)

xAxisArray = [xAxis1, xAxis2]
yAxisArray = [yAxis1, yAxis2]
yearAxis = [1, 12, 0, 1500]
# monthAxis = [1, 30, 0, 100]
# hourAxis = [0, 24, 0, 200]
# dayAxis = [0, 7, 0, 100]
plotChart(xAxisArray,yAxisArray, yearAxis)


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
