import pandas as pd
import os
from glob import glob

##############################################################################
# Author: Ted Ewing
# Date: 2021-09-07
# Description: Get profit per route with Blaze data. Requires the All Sales
# Reports for YTD. Takes completed time for each transaction, and buckets
# it into a standard route time, ex: 1-3 or 3-5.
##############################################################################

df = pd.DataFrame()
for x in glob('C:/Users/teddy/Documents/Python Scripts/KC/IC Data Collection/Data/All Sales Reports/*.csv'):
    if df.empty:
        df = pd.read_csv(x, skiprows=1)
    else:
        df = df.append(pd.read_csv(x, skiprows=1))

df.Date = pd.to_datetime(df.Date)
df['hour'] = df.Date.dt.hour
df['Retail Value'] = df['Retail Value'].str.strip('$').astype('float')
df['COGs'] = df['COGs'].str.strip('$').astype('float')

df['route'] = None

df.loc[df['hour'].between(13, 15), 'route'] = 'First Route (1-3pm)'
df.loc[df['hour'].between(15, 17), 'route'] = 'Second Route (3-5pm)'
df.loc[df['hour'].between(17, 19), 'route'] = 'Third Route (5-7pm)'
df.loc[df['hour'].between(19, 20), 'route'] = 'Fourth Route (7-8)'
df.loc[df['hour'].between(20, 21), 'route'] = 'Last Route'

df.loc[df['hour'].between(13, 15), 'route_nbr'] = 1
df.loc[df['hour'].between(15, 17), 'route_nbr'] = 2
df.loc[df['hour'].between(17, 19), 'route_nbr'] = 3
df.loc[df['hour'].between(19, 20), 'route_nbr'] = 4
df.loc[df['hour'].between(20, 21), 'route_nbr'] = 5

df.Date = df.Date.dt.date
routes = df.groupby(['Date', 'Employee', 'route', 'route_nbr']).agg({'Retail Value': 'sum', 'COGs': 'sum'})
routes['margin'] = routes['Retail Value'] - routes['COGs']
routes = routes.reset_index().sort_values(['Date', 'Employee', 'route_nbr'])

routes.to_clipboard()
routes.groupby(['route_nbr', 'route']).agg({'margin': 'mean'}).to_clipboard()