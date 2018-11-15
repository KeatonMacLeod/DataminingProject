from sqlalchemy import create_engine
import difflib
import re


# Utility function for converting database inputs to integers
def mk_int(s):
    s = s.strip()
    return int(s) if s else None


# Utility function for converting database inputs to strings
def mk_str(s):
    s = s.strip()
    return str(s) if s else None


def remove_special_characters(s):
    s = re.sub(r'([^\s\w]|_)+', '', s)
    return s


def find_most_similar_stops():
    engine = create_engine('mysql://root:casperto360flip@localhost/data_mining_bus_delays?charset=utf8')

    engine_result = engine.execute("SELECT Id, Location FROM location")
    result = engine_result.fetchall()

    location_ids = []
    location_names = []

    for i, location in enumerate(result):
        if location[1] is not None:
            location_ids.append(location[0])
            location_names.append(remove_special_characters(location[1].lower()))
        else:
            location_ids.append(None)
            location_names.append(None)

    engine_result = engine.execute("SELECT stop_code, stop_name FROM stops")

    result = engine_result.fetchall()

    original_stop_names = []  #Stores the original stop name without any modifications (such as .lower() or remove_special characters)
    stop_codes = []
    stop_names = []

    for i, stop in enumerate(result):
        if stop[0] is not None:
            stop_codes.append(stop[0])
            original_stop_names.append(stop[1])
            stop_names.append(remove_special_characters(stop[1].lower()))
        else:
            stop_codes.append(None)
            stop_names.append(None)

    num_processed = 0

    for i, location in enumerate(location_names):
        try:
            closest_modified_stop_name = difflib.get_close_matches(location, stop_names, 1)

            if closest_modified_stop_name:

                    closest_modified_stop_name = closest_modified_stop_name[0]
                    closest_modified_stop_name_index = stop_names.index(closest_modified_stop_name)

                    closest_stop_code = stop_codes[closest_modified_stop_name_index]
                    closest_stop_name = original_stop_names[closest_modified_stop_name_index]

                    engine.execute("UPDATE location SET `ClosestStopName` = \"" + closest_stop_name.replace("'", "\\'") + "\", `ClosestStopCode` = \"" + closest_stop_code + "\" WHERE `id` = " + str(location_ids[i]))
                    num_processed += 1

                    # print("Original: {} | Closest: {}".format(location, difflib.get_close_matches(location, stop_names, 1)))
                    if num_processed % 100 == 0:
                        print("Processed {} records".format(num_processed))

        except:
            pass
            print("Error")


def main():
    find_most_similar_stops()


if "__name__==__main__":
    main()
