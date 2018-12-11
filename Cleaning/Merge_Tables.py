from sqlalchemy import create_engine
import pandas as pd
import json

import time
def merge_tables(conn):
    #To select all records from delays and fill in the appropriate corresponding fields
    #need to join the location
    """df = pd.read_sql('SELECT R.Route, L.MostSimilarStopName, I.Incident, D.direction, V.Vehicle, Y.id, Time, `Report Date`, Day, `Min Delay`, `Min Gap`, year, month\
     FROM delay Y \
    Join Direction_caps D ON D.delay_id = Y.id\
    Join Vehicle V ON V.delay_id = Y.id\
    Join Incident I ON I.delay_id = Y.id\
    Join Route R ON R.delay_id = Y.id\
    Join Location L ON L.delay_id = Y.id\
    AND L.Similarity > 84
    LIMIT 5', conn)"""
    startTime = time.time()
    
    #delays = pd.read_sql('SELECT  Y.id as delay_id, Time, `Report Date`, Day, `Min Delay`, `Min Gap`, year, month\
    delays = pd.read_sql('SELECT  Y.id as delay_id, Time, `Report Date` as report_date, day, month, year, `Min Delay` as min_delay, `Min Gap` as min_gap\
    FROM delay Y WHERE `Min Delay` > 5', conn) 
    delays['Time'] = delays['Time'].astype(str)
    delays['Time'] = delays['Time'].str.split(' ').str[-1].str.split('.').str[0]
    delays['Hour'] = delays['Time'].str.split(':').str[0]
    delays['Minute'] = delays['Time'].str.split(':').str[1]
    print(time.time() - startTime); startTime = time.time()
    print("Delays read\n" + str(delays.head()))
    
    
    vehicle = pd.read_sql('SELECT delay_id, Vehicle from Vehicle', conn)
    print(time.time() - startTime); startTime = time.time()
    delays = delays.merge(vehicle, on='delay_id', how='inner')
    print(time.time() - startTime); startTime = time.time()
    print("Vehicles merged\n" + str(delays.head()))


    incident = pd.read_sql('SELECT delay_id, Incident from Incident', conn)
    print(time.time() - startTime); startTime = time.time()
    delays = delays.merge(incident, on='delay_id', how='inner')
    print(time.time() - startTime); startTime = time.time()
    print("Incidents merged\n" + str(delays.head()))


    route = pd.read_sql('SELECT delay_id, Route from Route', conn)
    print(time.time() - startTime); startTime = time.time()
    delays = delays.merge(route, on='delay_id', how='inner')
    print(time.time() - startTime); startTime = time.time()
    print("Routes merged\n" + str(delays.head()))

    
    direction = pd.read_sql('SELECT delay_id, Direction from Direction_caps', conn)
    print(time.time() - startTime); startTime = time.time()
    delays = delays.merge(direction, on='delay_id', how='inner')
    print(time.time() - startTime); startTime = time.time()
    print("Directions merged\n" + str(delays.head()))

    
    location = pd.read_sql('SELECT MostSimilarStopName, delay_id from Location WHERE similarity > 84', conn)
    print(time.time() - startTime); startTime = time.time()
    delays = delays.merge(location, on='delay_id', how='inner')
    print(time.time() - startTime); startTime = time.time()
    print("Locations merged\n" + str(delays.head()))
    print("Done merging all")

    delays.to_sql('delays_merged', conn)
    return


def do_merge(table1, table2):
    table1 = table1.merge(table2, left_index=True, right_on='delay_id', how='inner')

def connect():
    db_connection = "mysql+pymysql://root:root@localhost/bus_delays"
    conn = create_engine(db_connection)
    return conn

def main(): 
    #Connect to the database
    conn = connect()

    merge_tables(conn)
    
main()