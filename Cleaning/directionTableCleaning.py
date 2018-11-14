from sqlalchemy import create_engine
import pandas as pd
import json


DIRECTION_DATABASE = "direction"
update = True

def cleanUpdateCase(dbConn):
    print("Cleaning Database - Updating Case")
    check = dbConn.has_table("direction_caps")
    
    
    df = pd.read_sql("select * from "+ DIRECTION_DATABASE, dbConn)

    print("Sql read - Now printing head")
    print ( df.head() )

    print("Updating direction rows")
    df['Direction'] = df['Direction'].str.upper()
    
    pat = r"[\`\'\.\-\"\+ ]"
    df['Direction'] = df['Direction'].str.replace(pat, "")

    print("Reprinting head")
    print ( df.head() )

    with open('replacements.json') as data_file:
        data = json.load(data_file)


    for key in data:
        repls = data[key]
        for r in repls:
            print("Replacing {a} with {b}".format(a=r, b=key))
            df.loc[df['Direction'] == r, 'Direction'] = key

    keys = [key for key in data]
    print("The keys are %s" % (keys))

    df = df.where(df['Direction'].isin(keys))

    directions = df['Direction'].unique().tolist()
    print(directions)

    counts = {}
    countsDirection = {}
    
    for d in directions:
        
        #print("Now searching for {d} in the table".format(d=d))
        count = df[df.Direction == d].shape[0]
        counts[count] = counts.get(count, 0) + 1
        if count in countsDirection:
            countsDirection[count].append(d)
        else:
            countsDirection[count] = [d]

        # count = pd.read_sql("select count(Direction) from direction_caps WHERE Direction = '{d}'".format(d=d), dbConn)
        #print(count)
        #pass
    print("There were a total of {dd} distinct directions".format(dd=len(directions)))
    for key in counts:
        print("There were a total of {dd} distinct directions with {count} occurence".format(dd=counts[key], count=key))
        print("They are {s}".format(s = countsDirection[key]))
    print("There were a total of {dd} records".format(dd=df.shape[0]))

    # df.to_sql('direction_caps', dbConn)
    return

def connect():
    db_connection = "mysql+pymysql://root:root@localhost/bus_delays"
    conn = create_engine(db_connection)
    return conn

def main(): 
    #Connect to the database
    conn = connect()

    cleanUpdateCase(conn)
    
main()