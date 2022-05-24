import pandas as pd
from glob import glob
import os
from datetime import datetime, timedelta

month_map = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

def gather_sales(sls_dir):
    df = pd.DataFrame()
    for x in glob(os.path.join(sls_dir, '*.csv')):
        print(x)
        df = df.append(pd.read_csv(x, skiprows=1))
    df.Date = pd.to_datetime(df.Date)
    df['Retail Value'] = df['Retail Value'].str.strip('$').astype('float')
    df['Final Subtotal'] = df['Final Subtotal'].str.strip('$').astype('float')
    df['month'] = df.Date.dt.month
    df['year'] = df.Date.dt.year
    return df

def cust_lifetime_value(df):
    df = df.groupby(['Member ID', 'Member', 'Member Group', 'Date Joined']).agg({'Final Subtotal': 'sum', 'Date': 'max', 'Trans No.': 'nunique'})
    return df.sort_values('Final Subtotal', ascending=False)
sls_df = gather_sales(r'C:\Users\teddy\Documents\Python Scripts\KC\IC Data Collection\Data\All Sales Reports')
sls_df = sls_df.drop_duplicates(subset=['Date', 'Trans No.', 'Product SKU', 'Net Sales'])
sls_df = sls_df.sort_values(['Date', 'Member ID'])

sls_by_mon = sls_df.groupby(['year', 'month', 'Member ID']).agg({'Date': 'max'}).reset_index()
sls_by_mon['month'] = sls_by_mon['month'].map(month_map)

sls_grp = sls_df.groupby(['Date', 'Member', 'Member ID']).agg({'Retail Value': 'sum'}).reset_index()
sls_grp['date_diff'] = sls_grp.groupby('Member ID')['Date'].diff()
sls_grp = sls_grp.sort_values(['Member ID', 'Member', 'Date'])
sls_grp2 = sls_grp.groupby(['Member ID', 'Member']).agg({'Date': 'max'}).reset_index()
sls_grp2['churn'] = sls_grp2.apply(lambda x: 'Y' if datetime.today() - x['Date'] >= timedelta(days=90) else 'N', axis=1)
sls_grp2['month'] = sls_grp2['Date'].dt.month
sls_grp2['year'] = sls_grp2['Date'].dt.year
churn = sls_grp2.groupby(['year', 'month', 'churn']).count().reset_index()
sls_df['date_diff'] = sls_df.groupby('Member ID')['Date'].diff().reset_index(drop=True)
lt_val = cust_lifetime_value(sls_df)