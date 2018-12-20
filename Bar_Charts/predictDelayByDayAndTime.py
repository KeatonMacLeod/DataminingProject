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

def plotChart(xAxis, yAxis, xAxisPosition, color, my_xticks, xLabel, yLabel,figTitle, figName):
    year = 2014
    plot = plt.subplot(111)
    for i in range(0, len(xAxis)):
        plot.bar(np.array(xAxis[i])+xAxisPosition[i], yAxis[i], color=color[i], width=.2, label=str(year))
        year+=1
    x = list(range(0, len(xAxis[0])))
    plt.xticks(x, my_xticks)
    plt.xlabel(xLabel)
    plt.ylabel(yLabel) 
    plt.title(figTitle)
    plt.legend()
    # plt.savefig(figName)
    plt.show()
    

def plotChartByTime(xAxis, yAxis, axis): 
    plot = plt.subplot(111)
    color=['red', 'blue', 'orange', 'green']
    xAxisPosition = [-0.2, 0, 0.2, 0.4]
    year = 2014
    for i in range(0, len(xAxis)):
        plot.bar(np.array(xAxis[i])+xAxisPosition[i], yAxis[i], color=color[i], width=.2, label=str(year))
        year+=1
    x = [0,1,2,3]
    my_xticks = ['morning', 'afternoon', 'evening', 'night']
    plt.xticks(x, my_xticks)
    plt.xlabel('Time of Day')
    plt.ylabel('Delay Count')
    plt.legend()
    plt.title('Delay Count By Time of Day')
    plt.savefig("TimeOfDelayOccurences.png")
    plt.show()

def aggregateDelayCount(dictionary, xAxisString, xAxis):
    yAxis = [0]*len(xAxis)
    pos = -1
    for a in dictionary:
        if len(a) == 2:
            try:
                if a[0] in xAxisString:
                    pos = xAxisString.index(a[0])
                else:
                    pos = xAxisString.index(a[1])
                delayValue = int(dictionary[a])
                if yAxis[pos]>0:
                    yAxis[pos] = yAxis[pos]+delayValue
                else:
                    yAxis[pos] = delayValue
            except:
                print('Skipping data point due to parsing error')
    return yAxis


db_connection = "mysql+pymysql://root@localhost/bus_delays"

conn = create_engine(db_connection)

df1 = pd.read_sql("SELECT T.day, T.Incident FROM delays_binned as T WHERE T.year=2014", conn)
patterns1 = pyfpgrowth.find_frequent_patterns(processTimeDeltaObjs(df1.values), 10)

df2 = pd.read_sql("SELECT T.day, T.Incident FROM delays_binned as T WHERE T.year = 2015", conn)
patterns2 = pyfpgrowth.find_frequent_patterns(processTimeDeltaObjs(df2.values), 5)

df3 = pd.read_sql("SELECT T.day, T.Incident FROM delays_binned as T WHERE T.year=2016", conn)
patterns3 = pyfpgrowth.find_frequent_patterns(processTimeDeltaObjs(df3.values), 5)

df4 = pd.read_sql("SELECT T.day, T.Incident FROM delays_binned as T WHERE T.year=2017", conn)
patterns4 = pyfpgrowth.find_frequent_patterns(processTimeDeltaObjs(df4.values), 5)

xAxisString = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday']
xAxis = list(range(0, 7))

yAxis1 = aggregateDelayCount(patterns1, xAxisString, xAxis)
yAxis2 = aggregateDelayCount(patterns2, xAxisString, xAxis)
yAxis3 = aggregateDelayCount(patterns3, xAxisString, xAxis)
yAxis4 = aggregateDelayCount(patterns4, xAxisString, xAxis)
xAxisArrayByDay = [xAxis, xAxis, xAxis, xAxis]
yAxisArrayByDay = [yAxis1, yAxis2, yAxis3, yAxis4]
xAxisPositionByDay = [-.6,-0.4,-.2, 0,.2, 0.4, 0.6]
color=['red', 'blue', 'orange', 'green']
my_xticks = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri','Sat', 'Sun']
xLabel = 'Time of Day'
yLabel = 'Delay Count'
figTitle = 'Delay Count By Time of Day'
figName = 'Demo.png'
plotChart(xAxisArrayByDay,yAxisArrayByDay, xAxisPositionByDay, color, my_xticks, xLabel, yLabel,figTitle, figName)


#----------------------------------------------------------------------------------------

dff1 = pd.read_sql("SELECT T.time_of_day_bin, T.Incident FROM delays_binned as T WHERE T.year=2014", conn)
pattern1 = pyfpgrowth.find_frequent_patterns(processTimeDeltaObjs(dff1.values), 10)

dff2 = pd.read_sql("SELECT T.time_of_day_bin, T.Incident FROM delays_binned as T WHERE T.year = 2015", conn)
pattern2 = pyfpgrowth.find_frequent_patterns(processTimeDeltaObjs(dff2.values), 5)

dff3 = pd.read_sql("SELECT T.time_of_day_bin, T.Incident FROM delays_binned as T WHERE T.year=2016", conn)
pattern3 = pyfpgrowth.find_frequent_patterns(processTimeDeltaObjs(dff3.values), 5)

dff4 = pd.read_sql("SELECT T.time_of_day_bin, T.Incident FROM delays_binned as T WHERE T.year=2017", conn)
pattern4 = pyfpgrowth.find_frequent_patterns(processTimeDeltaObjs(dff4.values), 5)

xAxisString = ['morning', 'afternoon', 'evening', 'night']
xAxis = list(range(0, 4))

yAxis1 = aggregateDelayCount(pattern1, xAxisString, xAxis)
yAxis2 = aggregateDelayCount(pattern2, xAxisString, xAxis)
yAxis3 = aggregateDelayCount(pattern3, xAxisString, xAxis)
yAxis4 = aggregateDelayCount(pattern4, xAxisString, xAxis)
xAxisArrayByDay = [xAxis, xAxis, xAxis, xAxis]
yAxisArrayByDay = [yAxis1, yAxis2, yAxis3, yAxis4]
xAxisPositionByDay = [-0.2, 0, 0.2, 0.4]
xLabel = 'Day of Week'
figTitle = 'Delay Count By Day of Week'
plotChart(xAxisArrayByDay,yAxisArrayByDay, xAxisPositionByDay, color, my_xticks, xLabel, yLabel,figTitle, figName)



