from sqlalchemy import create_engine
from fuzzywuzzy import fuzz
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import *
import re


def make_lower_case(s):
    if s is not None:
        s = s.lower()
    return s


def remove_special_characters(s):
    if s is not None:
        s = re.sub(r'([^\s\w]|_)+', '', s)
    return s


def remove_spaces(s):
    if s is not None:
        s = "".join(s.split())
    return s

def remove_stop_words(s):
    stemmer = LancasterStemmer()
    if s is not None:
        tokenized_string = word_tokenize(s)
        s = [remove_spaces(stemmer.stem(w)) for w in tokenized_string if not w in (stopwords.words("english") + ["and"])]
        s = ' '.join(s)
    return s


def compare_original_location_with_difflib_stop():
    engine = create_engine('mysql://root:casperto360flip@localhost/data_mining_bus_delays?charset=utf8')

    engine_result = engine.execute("SELECT Id, Location, MostSimilarStopName FROM location")
    result = engine_result.fetchall()

    num_processed = 0

    for record in result:
        id = record[0]
        location_name = record[1]
        closest_stop_name = record[2]

        # Remove any of the unnecessary words within the stop names so we can see if the
        # important textual data exists within both location + stop
        location_name = remove_stop_words(remove_special_characters(make_lower_case(location_name)))
        closest_stop_name = remove_stop_words(remove_special_characters(make_lower_case(closest_stop_name)))

        # print("Location: {} | Closest Stop: {} | Ratio: {}".format(location_name, closest_stop_name, fuzz.token_set_ratio(location_name, closest_stop_name)))
        similarity = fuzz.token_set_ratio(location_name, closest_stop_name)
        engine.execute("UPDATE location SET `Similarity` = " + str(similarity) + " WHERE `id` = " + str(id))

        num_processed += 1

        if num_processed % 1000 == 0:
            print("Processed: {} records".format(num_processed))

def main():
    compare_original_location_with_difflib_stop()


if "__name__==__main__":
    main()
