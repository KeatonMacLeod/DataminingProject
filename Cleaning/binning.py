from sqlalchemy import create_engine
import pandas as pd
import sys

# Bin time of day into:
#     23-5 (night)
#     5-11 (morning)
#     11-17 (afternoon)
#     17-23 (evening)
#
# Bin delay into severity categories:
#     5-10 minutes short
#     10-20 minutes medium
#     20-30 minutes long
#     30-60 minutes severe
#     60+ minutes crippling


db_connection = "mysql+pymysql://root@localhost/Bus_Delays"

delays = pd.read_sql('SELECT * from delays_with_all_locations', db_connection)

df = delays.drop(columns=['Time', 'Minute', 'Vehicle', 'min_gap'])

# when binning time of day values must increase monotonically so shift them by one, then bin
df['Hour'] = df['Hour'].apply(lambda x: (int(x)+1)%24)
df['time_of_day_bin'] = pd.cut(df['Hour'], [-1,6,12,18,24], labels=['night','morning','afternoon','evening'])
df = df.drop(columns=['Hour'])

# bin delay
df['delay_severity'] = pd.cut(df['min_delay'], [0,10,20,30,60,1440], labels=['short','medium','long','severe', 'crippling'])
df = df.drop(columns=['min_delay'])

# clean up June/Jun
df = df.replace(to_replace=r'June', value='Jun', regex=True)

df.to_sql('delays_binned_with_all_locations', db_connection)
