from textwrap import fill
import pandas as pd
import os
from glob import glob
from datetime import datetime, timedelta

##############################################################################
# Author: Ted Ewing
# Date: 2021-10-25
# Description: Uses ALl Sales Reports from BLAZE to segment customer base and
# get customer churn by segment. Customers are segmented in 4 ways according
# to RFM scores. That is, Recency, Frequency, and Monetary. Also added AF as
# average frequency. This gives the average days between purchases. Recency
# is how recent the customer has purchased, frequency is the number of times
# the customer has purchased, and monetary is the total lifetime value of
# the customer. Finally, churn is modeled according to 90 days since last
# purchase in a pivot table. This allows us to model churn over time.
##############################################################################


def RScore(x, p, d):
    """Recency Score func, segments
    according to recency in 20% subsets

    Args:
        x (string): 'recency' for getting dict val
        p (dict): mapping for quantiles
        d (pd.Series): recency series

    Returns:
        int: recency segment value
    """

    if x <= d[p][0.2]:
        return 1
    elif x <= d[p][0.4]:
        return 2
    elif x <= d[p][0.6]:
        return 3
    elif x <= d[p][0.8]:
        return 4
    else:
        return 5


def FMScore(x, p, d):
    """Frequency and Monetary func, segements
    according to frequency and monetary values
    in 20% subsets

    Args:
        x (string): 'frequency' or 'monetary' for getting dict val
        p (dict): mapping for quantiles
        d (pd.Series): respective series for use

    Returns:
        int: frequency or monetary segement value
    """

    if x <= d[p][0.2]:
        return 5
    elif x <= d[p][0.4]:
        return 4
    elif x <= d[p][0.6]:
        return 3
    elif x <= d[p][0.8]:
        return 2
    else:
        return 1


def get_churn_days(row):
    """provides churn date according to most recent purchase
    for each customer segment. Each segment can be given
    different values for number of days until considered
    churned

    Args:
        row (pd.Series): a row from DF with apply

    Returns:
        int: days since last purchase that's considered
        churn. example: 90 since last purchase = cust churned
    """
    if row['m_quant'] == 1:
        return 90
    if row['m_quant'] in [2, 3, 4]:
        return 90
    else:
        return 90


month_map = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
    }
today = datetime.today()
df = pd.DataFrame()

# get all sales reports

for x in glob(
        'C:/Users/teddy/Documents/Python Scripts/KC/'
        'IC Data Collection/Data/All Sales Reports/*.csv'
        ):
    df = df.append(pd.read_csv(x, skiprows=1))

# drop duplicate orders if there are any, this helps
#  in reducing overhead with maintaining dates
# in the all sales reports. This will not be needed of
# course when BLAZE or in house DWs are
# in use. Strip out some stuff from string values for
# getting dollar amounts of orders and
# order quantities.

df = df.drop_duplicates(
    subset=['Date', 'Trans No.', 'Product SKU', 'Net Sales', 'Member ID']
    )
df.Date = pd.to_datetime(df.Date)
df['Net Sales'] = df['Net Sales'].str.strip('$').astype('float')
df['Quantity Sold'] = df['Quantity Sold'].str.strip('ea|g').astype('float')
df.loc[df['Marketing Source'].isna(), 'Marketing Source'] = 'None'

# get days between purchases for purch_freq DF
purch_freq = df.groupby(
    ['Member ID', 'Member', 'Date']
    ).agg(
        {'Net Sales': 'sum', 'Trans No.': 'max'}
        ).reset_index().sort_values(['Date', 'Member ID'])
purch_freq['day_btwn_purch'] = purch_freq.groupby(
    ['Member ID', 'Member']
    ).apply(
        lambda x: x['Date'].diff()
        ).reset_index().sort_values('Member ID')['Date']
purch_freq.sort_values(['Member ID', 'Date'], inplace=True)

# group customer line item purchases into single orders,
# since we dont need each product sold
# for this model

seg_sls = df.groupby(
    ['Member ID', 'Member', 'Date', 'Trans No.']
    ).agg(
        {'Net Sales': 'sum'}
        ).merge(
    purch_freq,
    how='inner',
    on=['Member ID', 'Trans No.']
    )
seg_sls.sort_values(['Member ID', 'Date'], inplace=True)

purch_freq['Avg Days Between Purch'] = purch_freq['day_btwn_purch'].apply(
    lambda x: x.days
    )
purch_freq = purch_freq.reset_index().groupby(['Member ID']).agg(
    {'Avg Days Between Purch': 'mean'}
    ).reset_index()

# combine some DFs to get the latest purchase date
# for each cust and combine with
# the number of days between purchases

cust_lst_purch = df.groupby(['Member ID', 'Member']).agg({'Date': 'max'})
cust_ttl = df.groupby(
    ['Member ID', 'Member', 'Date Joined', 'Marketing Source']
    ).agg({
            'Net Sales': 'sum',
            'Date': lambda x: (today-x.max()).days,
            'Trans No.': 'nunique'
            }).reset_index().sort_values(
                'Net Sales', ascending=False
                ).astype({'Date': int}).rename(
                columns={
                    'Net Sales': 'monetary',
                    'Date': 'recency',
                    'Trans No.': 'frequency'
                }
            ).merge(
                cust_lst_purch,
                how='left',
                on='Member ID'
                ).rename(columns={
                    'Date': 'Last Purchase Date'
                    }
                ).merge(
                    purch_freq,
                    how='left',
                    on='Member ID'
                )


cust_ttl['Last Purchase Date'] = pd.to_datetime(cust_ttl['Last Purchase Date'])

# get the quantiles of numeric columns
quantiles = cust_ttl.quantile(q=[0.2, 0.4, 0.6, 0.8]).to_dict()

seg_rfm = cust_ttl.copy()

# get quantile scores for each segment

seg_rfm['r_quant'] = seg_rfm['recency'].apply(
    RScore, args=('recency', quantiles,)
    )
seg_rfm['f_quant'] = seg_rfm['frequency'].apply(
    FMScore, args=('frequency', quantiles,)
    )
seg_rfm['m_quant'] = seg_rfm['monetary'].apply(
    FMScore, args=('monetary', quantiles,)
    )
seg_rfm['af_quant'] = seg_rfm['Avg Days Between Purch'].apply(
    RScore, args=('Avg Days Between Purch', quantiles,)
    )
seg_rfm['RFMScore'] = seg_rfm.r_quant.map(str) +\
    seg_rfm.f_quant.map(str) + seg_rfm.m_quant.map(str) +\
    seg_rfm.af_quant.map(str)
seg_rfm.sort_values('monetary', inplace=True, ascending=False)
seg_rfm = seg_rfm[[
    'Member ID', 'Member', 'Date Joined', 'Last Purchase Date',
    'Avg Days Between Purch', 'monetary',
    'recency', 'frequency', 'r_quant', 'f_quant', 'm_quant',
    'af_quant', 'RFMScore', 'Marketing Source'
    ]]


purch_seg = seg_sls.merge(
    seg_rfm[['Member ID', 'm_quant', 'af_quant', 'Last Purchase Date']],
    how='inner',
    on='Member ID'
    )
purch_seg['churn_days'] = purch_seg.apply(get_churn_days, axis=1)
purch_seg2 = purch_seg.groupby(
    ['Member ID', 'Member', 'churn_days', 'm_quant']
    ).agg({'Last Purchase Date': 'max'}).reset_index()

# compare today to the last time the customer purchased.
# If its over 90 days, then consider churn and
# return value of 1 so we can sum the number of churned customers over time,
# for ease of use


purch_seg2['churn'] = purch_seg2.apply(
    lambda x: 1 if (
        today - x['Last Purchase Date']
        ).days >= x['churn_days'] else 0, axis=1
    )
purch_seg2['month'] = purch_seg2['Last Purchase Date'].dt.month
purch_seg2['year'] = purch_seg2['Last Purchase Date'].dt.year
churn_mon = purch_seg2.groupby(
    ['year', 'month', 'm_quant']
    ).agg({'churn': ['sum']}).reset_index()
churn_mon.sort_values(['year', 'm_quant', 'month'])
churn_mon['yer_mon'] = churn_mon['year'].map(str) + '_' +\
    churn_mon['month'].map(str)
churn_piv = churn_mon.pivot_table(
    index=['year', 'month'],
    columns='m_quant',
    values='churn',
    fill_value=0
    )

# get new customers per month

new_cust = df.groupby(['Member ID', 'Member']).agg({'Date Joined': 'max'}).reset_index()
new_cust['Date Joined'] = pd.to_datetime(new_cust['Date Joined'])
new_cust['month'] = new_cust['Date Joined'].dt.month
new_cust['year'] = new_cust['Date Joined'].dt.year
new_cust_piv = new_cust.pivot_table(
    index=['year', 'month'],
    values='Date Joined',
    aggfunc='count',
    fill_value = 0
    ).reset_index().sort_values(['year', 'month'])

# marketing sources

mkt_src = seg_rfm.pivot_table(
    index=['m_quant', 'af_quant'],
    columns='Marketing Source',
    values='Member ID',
    aggfunc='nunique',
    fill_value=0
)
