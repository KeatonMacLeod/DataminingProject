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
    df_new = delays_2017.drop(columns=['level_0','index','delay_id'])
    df_older = delays_older.drop(columns=['level_0','index','delay_id'])

    # stringify values for pyfpgrowth
    df['Route'] = df['Route'].apply(str)
    df['year'] = df['year'].apply(str)
    df_new['Route'] = df_new['Route'].apply(str)
    df_new['year'] = df_new['year'].apply(str)
    df_older['Route'] = df_older['Route'].apply(str)
    df_older['year'] = df_older['year'].apply(str)



    patterns_new = pyfpgrowth.find_frequent_patterns(df_new.values, 50)
    patterns_older = pyfpgrowth.find_frequent_patterns(df_older.values, 150)

    #generate_association_rules

    set_new = set(patterns_new.keys())
    set_old = set(patterns_older.keys())

    inter = set_new & set_old

    print("Total patterns old: {}\nTotal patterns new: {}\nIntersect: {}\n".format(len(set_old), len(set_new), len(inter)))

#'report_date', 'day', 'month', 'year', 'Incident', 'Route', 'Direction', 'MostSimilarStopName', 'time_of_day_bin','delay_severity']


    # df1 = df.drop(columns=['report_date', 'MostSimilarStopName', 'Direction', 'month', 'year'])
    # TODO try to exclude either month or day
    # p1 = pyfpgrowth.find_frequent_patterns(df1.values, 50)

    # lens_p1 = len_counts(p1)



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
