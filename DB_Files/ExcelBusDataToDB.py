import pandas as pd
from sqlalchemy import create_engine


def read_in_bus_data_from_excel():

    # Define our starting year, ending year and all the months
    # we want to process
    delay_id = 1
    engine = create_engine('mysql://USER:PASS@localhost/data_mining_bus_delays?charset=utf8')

    start_year = 2014
    end_year = 2019

    # Iterate through start - end year and go through
    # all of the months reading data into the database
    for year in range(start_year, end_year):
        file_name = "Bus_" + str(year) + ".xlsx"
        bus_delay_excel_file = pd.ExcelFile(file_name)

        for sheet_name in bus_delay_excel_file.sheet_names:

            entire_data_frame = pd.read_excel(bus_delay_excel_file, sheet_name)
            month = sheet_name[:sheet_name.index(" ")]

            # Build up the dataframes with the corresponding columns for each table
            delay_dataframe = entire_data_frame[['Time', 'Report Date', 'Day', 'Min Delay', 'Min Gap']].copy()
            direction_dataframe = entire_data_frame[['Direction']].copy()
            incident_dataframe = entire_data_frame[['Incident']].copy()
            location_dataframe = entire_data_frame[['Location']].copy()
            route_dataframe = entire_data_frame[['Route']].copy()
            vehicle_dataframe = entire_data_frame[['Vehicle']].copy()

            # Add in additional columns to dataframe for joining on entire_data_frame records
            delay_dataframe['id'] = pd.Series(range(delay_id, delay_id + len(delay_dataframe)))
            delay_dataframe['year'] = pd.Series([year] * (len(delay_dataframe)))
            delay_dataframe['month'] = pd.Series([month] * (len(delay_dataframe)))
            direction_dataframe['id'] = pd.Series(range(delay_id, delay_id + len(incident_dataframe)))
            direction_dataframe['delay_id'] = pd.Series(range(delay_id, delay_id + len(incident_dataframe)))
            incident_dataframe['id'] = pd.Series(range(delay_id, delay_id + len(incident_dataframe)))
            incident_dataframe['delay_id'] = pd.Series(range(delay_id, delay_id + len(incident_dataframe)))
            location_dataframe['id'] = pd.Series(range(delay_id, delay_id + len(incident_dataframe)))
            location_dataframe['delay_id'] = pd.Series(range(delay_id, delay_id + len(incident_dataframe)))
            route_dataframe['id'] = pd.Series(range(delay_id, delay_id + len(incident_dataframe)))
            route_dataframe['delay_id'] = pd.Series(range(delay_id, delay_id + len(incident_dataframe)))
            vehicle_dataframe['id'] = pd.Series(range(delay_id, delay_id + len(incident_dataframe)))
            vehicle_dataframe['delay_id'] = pd.Series(range(delay_id, delay_id + len(incident_dataframe)))

            # Increment the delay_id
            delay_id = delay_id + len(delay_dataframe)

            # Write data frames to the database
            print("Inserting Data From: {}".format(sheet_name))
            delay_dataframe.to_sql(con=engine, name='delay', if_exists='append')
            direction_dataframe.to_sql(con=engine, name='direction', if_exists='append')
            incident_dataframe.to_sql(con=engine, name='incident', if_exists='append')
            location_dataframe.to_sql(con=engine, name='location', if_exists='append')
            route_dataframe.to_sql(con=engine, name='route', if_exists='append')
            vehicle_dataframe.to_sql(con=engine, name='vehicle', if_exists='append')


def main():
    read_in_bus_data_from_excel()


if "__name__==__main__":
    main()