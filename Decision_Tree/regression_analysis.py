from sklearn import tree
from sqlalchemy import create_engine
import pandas as pd
import time
import random

DB_CONNECTION = "mysql+pymysql://root:root@localhost/Bus_Delays"
DB_TABLE_NAME = "delays_binned_with_all_locations"
LIMIT = 10000

def generate_random_scenarios(unique_fields):
    to_p = []
    for i in range(50):
        new_list = []
        for j in range(len(unique_fields)):
            new_list.append(str(random.choice(unique_fields[j])))
        # print(new_list)
        to_p.append(new_list)
    return to_p

def generate_fields(fields):
    startTime = time.time()
    field_items = {}
    unique_fields =[]

    for f in fields:
        current_field = {}
        items = pd.read_sql("SELECT {f} from {d} LIMIT {l}".format(f=f, d=DB_TABLE_NAME, l=LIMIT), DB_CONNECTION)
        unique = items[f].unique().tolist()
        unique_fields.append(unique)
        for i in range(len(unique)):
            current_field[str(unique[i])] = i
        field_items[f] = current_field
    print("Done with generating fields "+ str(time.time() - startTime))
    return field_items, unique_fields

def build_tree(fields,  field_items, unique_fields):
    startTime = time.time()
    # print(unique_fields)
    data_set = []
    data_results = []
    keys_to_select = ', '.join(fields)
    keys_to_select += ', delay_severity'
    df = pd.read_sql("SELECT {f} from {d} LIMIT {l}".format(f=keys_to_select, d=DB_TABLE_NAME, l=LIMIT), DB_CONNECTION)

    for _, row in df.iterrows():
        correspond = []
        search_dict = {}
        for i  in range(len(fields)):
            f = fields[i]
            correspond.append( field_items[f]   [str(row[f])]    )
            search_dict[f] = str(row[f])
        # print(correspond)
        # print(row)
        data_set.append(correspond)
        query = ' & '.join(['{}==\'{}\''.format(k, v) for k, v in search_dict.items()])
        # print(query)
        equal_rows = df.query(query)
        """
            TODO Do something with the count and severity of the delay
            Use something like a heuristic calculator to determine whether a delay poses an issue.
            Currently only the count is used to determine if there are frequent delays given criteria
        """
        count = equal_rows.shape[0]
        # print(count.shape[0])
        data_results.append(count)

    print("Done with putting together data " + f + " " + str(time.time() - startTime))
    clf = tree.DecisionTreeRegressor()
    clf = clf.fit(data_set, data_results)

    print("Done with classification " + f + str(time.time() - startTime))
    tree.export_graphviz(clf, out_file="out.dot", feature_names=fields)
    print("Done with exporting " + f + str(time.time() - startTime))
    return clf

def predict_random_scenarios(fields, field_items, unique_fields, clf):
    startTime = time.time()
    to_p = generate_random_scenarios(unique_fields)

    print("Done with generating random scenarios " + str(time.time() - startTime))
    to_predict = []
    for t in to_p:
        ls = []
        for i in range(len(fields)):
            f = fields[i]
            ls.append(   field_items[f]    [t[i]]     )
        to_predict.append(ls)

    predictions = clf.predict(to_predict)
    for i in range(len(predictions)):
        print("{a} {b}".format(a=to_p[i], b=predictions[i]))
        
    print("Done predicting items "+ str(time.time() - startTime))
    return

def main():
    fields = ["month", "day", "route", "time_of_day_bin"]
    field_items, unique_fields = generate_fields(fields)
    clf = build_tree(fields, field_items, unique_fields)
    predict_random_scenarios(fields, field_items, unique_fields, clf)

main()