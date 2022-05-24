import re
import pandas as pd
import os
import requests
from datetime import datetime

def get_active_deliveries(
        start_date: datetime=datetime.today().strftime('%m/%d/%Y'),
        end_date: datetime=None
        ) -> pd.DataFrame:

    url = "https://api-or.metrc.com/sales/v1/deliveries/active"

    headers = {
    'Authorization': 'Basic '
    }
    params = {'licenseNumber': '050-10055625B58', 'salesDateStart': start_date, 'salesDateEnd': end_date}

    response = requests.get(url, headers=headers, params=params)
    if response.ok:
        return pd.json_normalize(response.json())
    else:
        return (
            f'Error retrieving Active Deliveries --'
            f'Error code: {response.status_code} with reason {response.reason}'
            )

def complete_deliveries(row: pd.Series) -> None:
    d = {'Id': row['Id'], 'ActualArrivalDateTime': row['SalesDateTime']}
    
    url = f"https://api-or.metrc.com//sales/v1/delivery/{d['Id']}"
    headers = {
        'Authorization': 'Basic '
        }    
    params = {'licenseNumber': '050-10055625B58'}

    response = requests.get(url, headers=headers, params=params)
    d['AcceptedPackages'] = [x.get('PackageLabel') for x in response.json().get('Transactions')]
    
    put_response = request_completion(payload=[d], headers=headers, params=params)

    if not response.ok:
        return f'error completing manifest'

def request_completion(payload, headers, params):
    url = 'https://api-or.metrc.com/sales/v1/deliveries/complete'
    r = requests.put(url, params=params, headers=headers, data=payload)
    print(r.status_code, r.reason)


def main():
    deliveries = get_active_deliveries()
    deliveries.apply(complete_deliveries, axis='columns')
    return df

df = main()