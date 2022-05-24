import pandas as pd
from glob import glob


df = pd.DataFrame()
for x in glob('/home/ted/Documents/kc/SLS-2020/*.csv'):
    df = df.append(pd.read_csv(x, skiprows=1))
df['Member'] = df['Member'].str.replace('^[a-zA-z]', '')
df['Date'] = pd.to_datetime(df['Date']).dt.date
df['Date Joined'] = pd.to_datetime(df['Date Joined']).dt.date
df[['Quantity', 'Units']] = df['Quantity Sold'].str.split(' ', expand=True)
df2 = df[['Date',
          'Product Name',
          'Product Category',
          'Brand Name',
          'Vendor',
          'Member',
          'Quantity Sold',
          'COGs',
          'Retail Value',
          'Net Sales',
          'Subtotal',
          'Total Discount',
          'Payment Type',
          'Promotion(s)',
          'Marketing Source',
          'Zip Code',
          'Date Joined',
          'Member ID',
          'Quantity',
          'Units']]
