import pandas as pd
from google.oauth2 import service_account
import pandas_gbq as pdbq

creds = service_account.Credentials.from_service_account_file(
    '/home/ted/Documents/kc/cust_dash/bq-test-proj-349123-8e736b8a6e2d.json')

sls = pd.read_csv(
    '/home/ted/Documents/kc/cust_dash/COMPLETED_SALES_DETAILS_REPORT (2).csv', skiprows=1)

sls['Member'] = sls['Member'].str.replace('[^a-zA-z ]', '')
sls['Date'] = pd.to_datetime(sls['Date']).dt.date
sls['Date Joined'] = pd.to_datetime(sls['Date Joined']).dt.date
sls[['Quantity', 'Units']] = sls['Quantity Sold'].str.split(' ', expand=True)
for x in ['COGs', 'Retail Value', 'Subtotal', 'Net Sales', 'Total Discount']:
    sls[x] = sls[x].str.strip('$')
sls2 = sls[['Date',
            'Trans No.',
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

sls2['Date'] = pd.to_datetime(sls2['Date']).dt.date
sls2['Date Joined'] = pd.to_datetime(sls2['Date Joined']).dt.date
sls2 = sls2.astype({
    'COGs': 'float',
    'Retail Value': 'float',
    'Net Sales': 'float',
    'Subtotal': 'float',
    'Total Discount': 'float',
    'Quantity': 'float',
    'Zip Code': str,
    'Date': str,
    'Date Joined': str
})


sls2.columns = ['Date',
                'Trans_No_',
                'Product_Name',
                'Product_Category',
                'Brand_Name',
                'Vendor',
                'Member',
                'Quantity_Sold',
                'COGs',
                'Retail_Value',
                'Net_Sales',
                'Subtotal',
                'Total_Discount',
                'Payment_Type',
                'Promotion_s_',
                'Marketing_Source',
                'Zip_Code',
                'Date_Joined',
                'Member_ID',
                'Quantity',
                'Units']


project_id = 'bq-test-proj-349123'
pdbq.to_gbq(
    sls2, 'test_txn.kc_txns_2020_2021_2', project_id=project_id, if_exists='append'
)
