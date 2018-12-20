from sqlalchemy import create_engine
import pandas as pd
import pyfpgrowth

def main():
    count_patterns()

def count_patterns():
    db_connection = "mysql+pymysql://root@localhost/BusDelays"

    results = {}
    counts = {}

    MINSUP = 50

    years = ['2014', '2015', '2016', '2017']

    # types of (maximal) patterns we're interested in (all with delay severity)
    pat_types = ['Route,Direction,time_of_day_bin',
                 'MostSimilarStopName,Direction,time_of_day_bin',
                 'Route,Incident,time_of_day_bin',
                 'day,time_of_day_bin',
                 'month,Incident',
                 'Direction,time_of_day_bin',
                 'Route,time_of_day_bin',
                 'Route,Direction',
                 'MostSimilarStopName,time_of_day_bin',
                 'MostSimilarStopName,Direction',
                 'Incident,time_of_day_bin',
                 'Route,Incident',
                 'day',
                 'month',
                 'Route',
                 'MostSimilarStopName',
                 'time_of_day_bin',
                 'Incident']

    # which columns to drop to get said pattern types
    col_drop_types = [['day','month','year','Incident','MostSimilarStopName'],
                      ['day','month','year','Incident','Route'],
                      ['day','month','year','Direction','MostSimilarStopName'],
                      ['month','year','Incident','Route','Direction','MostSimilarStopName'],
                      ['day','year','Route','Direction','MostSimilarStopName','time_of_day_bin'],
                      ['day','month','year','Incident','Route','MostSimilarStopName'],
                      ['day','month','year','Incident','Direction','MostSimilarStopName'],
                      ['day','month','year','Incident','MostSimilarStopName','time_of_day_bin'],
                      ['day','month','year','Incident','Route','Direction'],
                      ['day','month','year','Incident','Route','time_of_day_bin'],
                      ['day','month','year','Route','Direction','MostSimilarStopName'],
                      ['day','month','year','Direction','MostSimilarStopName','time_of_day_bin'],
                      ['month','year','Incident','Route','Direction','MostSimilarStopName','time_of_day_bin'],
                      ['day','year','Incident','Route','Direction','MostSimilarStopName','time_of_day_bin'],
                      ['day','month','year','Incident','Direction','MostSimilarStopName','time_of_day_bin'],
                      ['day','month','year','Incident','Route','Direction','time_of_day_bin'],
                      ['day','month','year','Incident','Route','Direction','MostSimilarStopName'],
                      ['day','month','year','Route','Direction','MostSimilarStopName','time_of_day_bin']]

    for year in years:
        results[year] = {}
        counts[year] = {}
        delays = pd.read_sql('SELECT * from delays_binned where year = {}'.format(year), db_connection)

        # get rid of fields we don't need
        df = delays.drop(columns=['level_0', 'index', 'delay_id', 'report_date'])

        # stringify values for pyfpgrowth
        df['Route'] = df['Route'].apply(str)
        df['year'] = df['year'].apply(str)

        for i in range(0,len(pat_types)):
            # drop cols
            df_dropped = df.drop(columns=col_drop_types[i])

            # find patterns store results
            fps = pyfpgrowth.find_frequent_patterns(df_dropped.values, MINSUP)

            # only keep max length for each category
            results[year][pat_types[i]] = { x:fps[x] for x in fps.keys() if len(x) > len(pat_types[i].split(','))}
            # store counts as we go
            counts[year][pat_types[i]] = len(results[year][pat_types[i]])


    # print numbers
    for year, pattern_types in counts.items():
        print(year)
        total = 0
        for pattern_type, count in pattern_types.items():
            # print("{}: {}".format( pattern_type, str(count)))
            total += count
        print(total)

    # intersection counts
    inter_sets = {}
    inter_counts = {}

    prev_years = results[years[0]]
    prev_year_label = years[0]

    for i in range(1,len(years)):
        prev_year_label = prev_year_label + "^" + years[i]
        inter_sets[prev_year_label] = {}
        inter_counts[prev_year_label] = {}
        for pat in pat_types:
            inter_sets[prev_year_label][pat] = set(prev_years[pat]) & set(results[years[i]][pat])
            inter_counts[prev_year_label][pat] = len(inter_sets[prev_year_label][pat])
        prev_years = inter_sets[prev_year_label]


    # check intersect of 2015^2016^2017 (i.e. ditch oldest year)
    inter_sets['2015^2016^2017'] = {}
    inter_counts['2015^2016^2017'] = {}
    inter_sets['2014^2016^2017'] = {}
    inter_counts['2014^2016^2017'] = {}
    inter_sets['2014^2015^2017'] = {}
    inter_counts['2014^2015^2017'] = {}
    inter_sets['2016^2017'] = {}
    inter_counts['2016^2017'] = {}
    inter_sets['2015^2016'] = {}
    inter_counts['2015^2016'] = {}
    inter_sets['2015^2017'] = {}
    inter_counts['2015^2017'] = {}
    inter_sets['2014^2016'] = {}
    inter_counts['2014^2016'] = {}
    inter_sets['2014^2017'] = {}
    inter_counts['2014^2017'] = {}
    for pat in pat_types:
        s14 = set(results['2014'][pat])
        s15 = set(results['2015'][pat])
        s16 = set(results['2016'][pat])
        s17 = set(results['2017'][pat])
        inter_sets['2015^2016^2017'][pat] = s15 & s16 & s17
        inter_counts['2015^2016^2017'][pat] = len(inter_sets['2015^2016^2017'][pat])
        inter_sets['2014^2016^2017'][pat] = s14 & s16 & s17
        inter_counts['2014^2016^2017'][pat] = len(inter_sets['2014^2016^2017'][pat])
        inter_sets['2014^2015^2017'][pat] = s14 & s15 & s17
        inter_counts['2014^2015^2017'][pat] = len(inter_sets['2014^2015^2017'][pat])
        inter_sets['2016^2017'][pat] = s16 & s17
        inter_counts['2016^2017'][pat] = len(inter_sets['2016^2017'][pat])
        inter_sets['2015^2016'][pat] = s15 & s16
        inter_counts['2015^2016'][pat] = len(inter_sets['2015^2016'][pat])
        inter_sets['2015^2017'][pat] = s15 & s17
        inter_counts['2015^2017'][pat] = len(inter_sets['2015^2017'][pat])
        inter_sets['2014^2016'][pat] = s14 & s16
        inter_counts['2014^2016'][pat] = len(inter_sets['2014^2016'][pat])
        inter_sets['2014^2017'][pat] = s14 & s17
        inter_counts['2014^2017'][pat] = len(inter_sets['2014^2017'][pat])


    # print intersect counts
    for intersect, pattern_types in inter_counts.items():
        print(intersect)
        total = 0
        for pattern_type, count in pattern_types.items():
            # print("{}: {}".format( pattern_type, str(count)))
            total += count
        print(total)



def do_patterns(years):
    results = {}
    counts = {}
    for year in years:
        results[year] = {}
        counts[year] = {}
        delays = pd.read_sql('SELECT * from delays_binned where year = {}'.format(year), db_connection)
        # get rid of fields we don't need
        df = delays.drop(columns=['level_0', 'index', 'delay_id', 'report_date'])
        # stringify values for pyfpgrowth
        df['Route'] = df['Route'].apply(str)
        df['year'] = df['year'].apply(str)
        for i in range(0,len(pat_types)):
            # drop cols
            df_dropped = df.drop(columns=col_drop_types[i])
            # find patterns store results
            fps = pyfpgrowth.find_frequent_patterns(df_dropped.values, MINSUP)
            # only keep max length for each category
            results[year][pat_types[i]] = { x:fps[x] for x in fps.keys() if len(x) > len(pat_types[i].split(','))}
            # store counts as we go
            counts[year][pat_types[i]] = len(results[year][pat_types[i]])
    return counts


if __name__ == '__main__':
    main()
