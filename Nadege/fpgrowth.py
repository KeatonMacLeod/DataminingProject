from sqlalchemy import create_engine
import pandas as pd
import pyfpgrowth
import numpy as np

def main():
    db_connection = "mysql+pymysql://root@localhost/BusDelays"

    delays = pd.read_sql('SELECT * from delays_binned', db_connection)
    delays_2017 = pd.read_sql('SELECT * from delays_binned where year = 2017', db_connection)
    delays_older = pd.read_sql('SELECT * from delays_binned where year < 2017', db_connection)

    df = delays.drop(columns=['index','delay_id'])
    df_new = delays_2017.drop(columns=['index','delay_id'])
    df_older = delays_older.drop(columns=['index','delay_id'])

    # stringify values for pyfpgrowth
    df['Route'] = df['Route'].apply(str)
    df['year'] = df['year'].apply(str)
    df_new['Route'] = df_new['Route'].apply(str)
    df_new['year'] = df_new['year'].apply(str)
    df_older['Route'] = df_older['Route'].apply(str)
    df_older['year'] = df_older['year'].apply(str)



    patterns_new = pyfpgrowth.find_frequent_patterns(df_new.values, 100)
    patterns_older = pyfpgrowth.find_frequent_patterns(df_older.values, 100)


    df1 = df.drop(columns=['report_date', 'Incident', 'MostSimilarStopName', 'Route'])
    p1 = pyfpgrowth.find_frequent_patterns(df1.values, 100)

    lens_p1 = len_counts(p1)



def len_counts(patterns):
    lens = {}
    for p in patterns:
        if len(p) not in lens.keys():
            lens[len(p)] = 1
        else:
            lens[len(p)] += 1
    return lens

def print_pattern_len(patterns, strlen):
    for p in patterns:
        if len(p) == strlen:
            print("{}: {}".format( p, patterns[p]))


if __name__ == '__main__':
    main()
