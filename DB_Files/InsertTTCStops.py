from sqlalchemy import create_engine


# Utility function for converting database inputs to integers
def mk_int(s):
    s = s.strip()
    return int(s) if s else None


# Utility function for converting database inputs to strings
def mk_str(s):
    s = s.strip()
    return str(s) if s else None


# Process the data in the "stops.txt" file from Toronto's open data portal, which is used to populate
# the list of all the Toronto's transit stops
def read_in_data_from_file():
    engine = create_engine('mysql://USER:PASS@localhost/data_mining_bus_delays?charset=utf8')

    with open("stops.txt") as f:
        for i, line in enumerate(f.readlines()):
            if i is not 0:
                row = line.strip().split(",")
                engine_result = engine.execute("SELECT * FROM stops WHERE stop_name LIKE (%s)", row[2])
                result = engine_result.fetchone()

                # Make sure all the stop names inserted are unique
                if result is None:
                    engine.execute("""INSERT INTO stops(stop_id, stop_code, stop_name, stop_desc, stop_latitude, stop_longitude, zone_id, stop_url, location_type, parent_station, wheelchair_boarding) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                   mk_int(row[0]), mk_str(row[1]), mk_str(row[2]).replace('"', '\\"'), mk_str(row[3]), mk_str(row[4]), mk_str(row[5]), mk_int(row[6]), mk_str(row[7]), mk_str(row[8]), mk_int(row[9]), mk_int(row[10]))


def main():
    read_in_data_from_file()


if "__name__==__main__":
    main()
