from sqlalchemy import create_engine
import pandas as pd
import pyfpgrowth
import numpy as np

db_connection = "mysql+pymysql://root@localhost/BusDelays"

delays = pd.read_sql('SELECT * from delays_binned', db_connection)

df = delays.drop(columns=['index','delay_id'])

# stringify values for pyfpgrowth
df['Route'] = df['Route'].apply(str)
df['year'] = df['year'].apply(str)

patterns = pyfpgrowth.find_frequent_patterns(df.values, 100)

# print(patterns)

# filter patterns so that they contain at least severity info (?)
