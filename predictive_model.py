import pandas as pd
import pyfpgrowth
from prettytable import PrettyTable
import numpy as np


def main():

    # Intialize the database connection and the list of years for which data is available
    db_connection = "mysql+pymysql://root@localhost/BusDelays"
    toronto_bus_data_years = ["2014", "2015", "2016", "2017"]

    previous_years_frequent_patterns = {}
    frequent_pattern_min_sup = 25

    # Gather each year's frequent bus delay patterns and store them in "all_years_frequent_patterns"
    # for analysis on interesting patterns
    for year in toronto_bus_data_years:
        # Gather the dataframe representing "that year's" bus delay data
        df_year = pd.read_sql('SELECT * from delays_binned where year = ' + year, db_connection)

        # Remove "uninteresting" columns from potential "interesting" datasets
        df_year_filtered = df_year.drop(columns=['index', 'delay_id', 'year', 'report_date', 'Direction', 'Route'])
        df_year_frequent_patterns = pyfpgrowth.find_frequent_patterns(df_year_filtered.values, frequent_pattern_min_sup)
        previous_years_frequent_patterns[year] = df_year_frequent_patterns

    # Create the "future year's" patterns and remove the "future year's" patterns
    # from the previous_year_list_frequent_pattern
    future_year = "2017"
    future_year_frequent_patterns = {future_year: previous_years_frequent_patterns[future_year]}
    del previous_years_frequent_patterns[future_year]

    frequent_pattern_min_length = 5

    # Find out the interesting rules between previous data and "future" data
    all_frequent_previous_years_patterns = find_frequent_patterns_occuring_in_all_previous_years(previous_years_frequent_patterns, frequent_pattern_min_length)
    match_previous_and_future_frequently_occuring_patterns(all_frequent_previous_years_patterns, future_year_frequent_patterns)
    analyze_frequent_patterns(all_frequent_previous_years_patterns)
    # find_similarities_and_differences(previous_years_frequent_patterns, future_year_frequent_patterns, frequent_pattern_min_length)


# Find all the similar patterns with a "frequent_pattern_minimum_length" of N where previous year's patterns
# are indicative of patterns in a future year
def find_frequent_patterns_occuring_in_all_previous_years(previous_years_frequent_patterns, frequent_pattern_min_length):
    all_frequent_previous_years_patterns = {}

    for pattern, count in previous_years_frequent_patterns["2014"].items():
        if pattern in previous_years_frequent_patterns["2015"] and pattern in previous_years_frequent_patterns["2016"] and len(pattern) == frequent_pattern_min_length:
            all_frequent_previous_years_patterns[pattern] = {}
            all_frequent_previous_years_patterns[pattern]["2014"] = previous_years_frequent_patterns["2014"][pattern]
            all_frequent_previous_years_patterns[pattern]["2015"] = previous_years_frequent_patterns["2015"][pattern]
            all_frequent_previous_years_patterns[pattern]["2016"] = previous_years_frequent_patterns["2016"][pattern]

    return all_frequent_previous_years_patterns


def match_previous_and_future_frequently_occuring_patterns(all_frequent_previous_years_patterns, future_year_frequent_patterns):
    # Copy over the values
    all_years_frequently_occuring_patterns = {}

    for previous_years_pattern, count in all_frequent_previous_years_patterns.items():
        if previous_years_pattern in future_year_frequent_patterns["2017"]:
            all_years_frequently_occuring_patterns[previous_years_pattern] = {}
            all_years_frequently_occuring_patterns[previous_years_pattern] = all_frequent_previous_years_patterns[previous_years_pattern]
            all_years_frequently_occuring_patterns[previous_years_pattern]["2017"] = future_year_frequent_patterns["2017"][previous_years_pattern]
        else:
            all_years_frequently_occuring_patterns[previous_years_pattern] = {}
            all_years_frequently_occuring_patterns[previous_years_pattern] = all_frequent_previous_years_patterns[previous_years_pattern]
            all_years_frequently_occuring_patterns[previous_years_pattern]["2017"] = -1

    table = PrettyTable(['Frequent Patterns', '2017 (Future Year)', '2016', '2015', '2014'])

    for pattern, year_dictionary in all_years_frequently_occuring_patterns.items():
        table.add_row([pattern,
                       all_years_frequently_occuring_patterns[pattern]["2017"],
                       all_years_frequently_occuring_patterns[pattern]["2016"],
                       all_years_frequently_occuring_patterns[pattern]["2015"],
                       all_years_frequently_occuring_patterns[pattern]["2014"]])


    table.sortby = "2017 (Future Year)"
    table.reversesort = True
    print(table)
    return all_years_frequently_occuring_patterns


def analyze_frequent_patterns(all_years_frequently_occuring_patterns):
    patterns_matching_future_year = {}  # Previous patterns which successfully predicted frequent patterns in the future year
    patterns_differing_from_future_year = {}  # Previous patterns which unsuccessfully predicted frequent patterns in the future year
    for pattern in all_years_frequently_occuring_patterns:
        if all_years_frequently_occuring_patterns[pattern]["2017"] is -1:
            patterns_differing_from_future_year[pattern] = all_years_frequently_occuring_patterns[pattern]
        else:
            patterns_matching_future_year[pattern] = all_years_frequently_occuring_patterns[pattern]

    print("{} future patterns were predicted as being frequent correctly when all previous years had the same pattern".format(len(patterns_matching_future_year)))
    print("{} future patterns were predicted as being frequent incorrectly when all previous years had the same pattern".format(len(patterns_differing_from_future_year)))


# Find all the similar patterns with a "frequent_pattern_minimum_length" of N where previous year's patterns
# are indicative of patterns in a future year
def find_similarities_and_differences(previous_years_frequent_patterns, future_year_frequent_patterns, frequent_pattern_min_length):
    table = PrettyTable(['Frequent Pattern', '2017 (Future Year)', '2016', '2015', '2014'])

    for fut_year, fut_frequent_patterns in future_year_frequent_patterns.items():
        for fut_pattern, fut_count in fut_frequent_patterns.items():

            # Patterns predicting future patterns
            predicted_patterns = {"2014": "--", "2015": "--", "2016": "--"}
            pattern_predicted = False

            for prev_year, prev_frequent_patterns in previous_years_frequent_patterns.items():
                for prev_pattern, prev_count in prev_frequent_patterns.items():
                    if sorted(fut_pattern) == sorted(prev_pattern) and len(fut_pattern) == frequent_pattern_min_length:
                        predicted_patterns[prev_year] = str(prev_count)
                        pattern_predicted = True

            if pattern_predicted:
                table.add_row([fut_pattern, fut_count, predicted_patterns["2016"], predicted_patterns["2015"], predicted_patterns["2014"]])

    table.sortby = "2017 (Future Year)"
    table.reversesort = True
    print(table)


if __name__ == '__main__':
    main()
