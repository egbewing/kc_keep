import pandas as pd
import requests
import os
from gspread_pandas import Spread
from datetime import datetime, timedelta

def get_blz_batch_qty(skip: int=0, inventory: str='safe') -> pd.DataFrame:
    """
    Retrieve batch quantities from BLAZE.
    Args:
        skip (int, optional): nbr records to skip at API call. Defaults to 0.
        inventory (str, optional): inventory to query. Defaults to 'safe'.
    Returns:
        pd.DataFrame: batch quantity df
    """
    inventories = {
        'safe': '5d26ca35002ec407fccc9e39',
        'backstock': '5d72f55b0964cc083b14fce8',
        'ict1': '61031630142eec3c1122ebb3',
        'exchange': '5d26ca35002ec407fccc9e3c'
        }
    url = "https://api.partners.blaze.me/api/v1/partner/store/batches/quantities"
    headers = {
        'partner_key': os.getenv('blz_partner_key'),
        'Authorization': os.getenv('blz_api_key')
        }
    params = {'inventoryId': inventories.get(inventory), 'start': skip}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        dat = pd.DataFrame().from_records(response.json().get('values'))
        if skip >= response.json().get('total'):
            return dat
        else:
            return (
                pd.concat([
                    dat,
                    get_blz_batch_qty(
                        skip=skip + response.json().get('limit'),
                        inventory='safe'
                        )
                    ])
                )
    else:
        return f'Error retrieving inventory: {inventory} with '
        f'id {inventories.get(inventory)} with status code {response.status_code}'


def get_products(skip: int=0) -> pd.DataFrame:
    """
    Retrieve all products from BLAZE API.
    Args:
        skip (int, optional): Nbr records to skip at API call. Defaults to 0.
    Returns:
        pd.DataFrame: df of all products
    """
    url = "https://api.partners.blaze.me/api/v1/partner/products"

    headers = {
        'partner_key': os.getenv('blz_partner_key'),
        'Authorization': os.getenv('blz_api_key')
        }
    params = {'skip': skip}

    response = requests.get(url, headers=headers,params=params)
    if response.status_code == 200:
        dat = pd.DataFrame().from_records(response.json().get('values'))
        if skip  >= response.json().get('total'):
            return dat
        else:
            return(
                pd.concat([
                    dat,
                    get_products(skip=skip + response.json().get('limit'))
                ])
            )
    else:
        return f'Error retrieving products: with status code {response.status_code}'


def get_vendors(skip: int=0) -> pd.DataFrame:
    """
    Get all vendors from BLAZE, recursively
    Args:
        skip (int, optional): nbr records to skip. Defaults to 0.
    Returns:
        pd.DataFrame: vendors data
    """
    url = "https://api.partners.blaze.me/api/v1/partner/vendors"

    headers = {
        'partner_key': os.getenv('blz_partner_key'),
        'Authorization': os.getenv('blz_api_key')
        }
    params = {'skip': skip}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        dat = pd.DataFrame().from_records(response.json().get('values')).rename(
            columns={
                'name': 'vendor_name'
                }
            ).drop_duplicates('id')
        if skip >= response.json().get('total'):
            return dat
        else:
            return(
                pd.concat([
                    dat,
                    get_vendors(skip=skip + response.json().get('limit'))
                ])
            )
    else:
        return f'Error retrieving products: with status code {response.status_code}'


def get_brands(skip: int=0) -> pd.DataFrame:
    """
    Get all brands from BLAZE API recursively
    Args:
        skip (int, optional): nbr of records to skip (api param). Defaults to 0.
    Returns:
        pd.DataFrame: Brands data
    """
    url = "https://api.partners.blaze.me/api/v1/partner/store/inventory/brands"

    headers = {
        'partner_key': os.getenv('blz_partner_key'),
        'Authorization': os.getenv('blz_api_key')
        }
    params = {'limit': 200, 'start': skip}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        dat = pd.DataFrame().from_records(response.json().get('values')).rename(
            columns={
                'name': 'brand_name', 'id': 'brand_id'
                }
            ).drop_duplicates('brand_id')
        if skip >= response.json().get('total'):
            return dat
        else:
            return(
                pd.concat([
                    dat,
                    get_brands(skip=skip + response.json().get('limit'))
                ])
            )
    else:
        return f'Error retrieving products: with status code {response.status_code}'

def combine_txns(s):
    """For get_sls(), but is not in use. See comments in main()
    Args:
        s ([type]): [description]
    Returns:
        [type]: [description]
    """
    df = pd.DataFrame()
    for x in s:
        if df.empty:
            df = x
        else:
            df = pd.concat([df, x])
    return df


def get_sls(
        start: str=(datetime.today() - timedelta(days=1)).strftime('%m/%d/%Y'),
        end: str=datetime.today().strftime('%m/%d/%Y'),
        skip=0
        ):
    """Currently unused, needs revision. See comments in main()
    Args:
        start (str, optional): [description]. Defaults to (datetime.today() - timedelta(days=1)).strftime('%m/%d/%Y').
        end (str, optional): [description]. Defaults to datetime.today().strftime('%m/%d/%Y').
        skip (int, optional): [description]. Defaults to 0.
    Returns:
        [type]: [description]
    """

    url = "https://api.partners.blaze.me/api/v1/partner/transactions"
    headers = {
            'partner_key': os.getenv('blz_partner_key'),
            'Authorization': os.getenv('blz_api_key')
            }
    params = {'startDate': start, 'endDate': end, 'skip': skip}
    response = requests.get(url, headers=headers, params=params)
    if response.ok:
        dat = pd.DataFrame().from_dict(response.json().get('values'))
        itms = dat.cart.apply(lambda x: x.get('items'))
        itms = itms.apply(lambda x: pd.DataFrame().from_records(x))
        itms = combine_txns(itms)
        dat['joinid'] = dat.id.apply(lambda x: x[0:-3])
        dat = dat.rename(columns={'id': 'txn_id'})
        dat['created_dt'] = dat.created.apply(lambda x: datetime.fromtimestamp(x/1000))
        dat['completedTime'] = dat['completedTime'].apply(lambda x: datetime.fromtimestamp(x/1000))
        itms['joinid'] = itms.id.apply(lambda x: x[0:-3])
        ln_itm_txns = dat.merge(
            itms,
            how='left',
            on='joinid'
            )
        ln_itm_txns = ln_itm_txns[[
            'created_dt',
            'completedTime',
            'transNo',
            'productId',
            'quantity'
            ]]
        if skip + response.json().get("limit") >= response.json().get('total'):
            return ln_itm_txns
        else:
            return pd.concat([
                ln_itm_txns,
                get_sls(start=start, end=end, skip=skip + response.json().get('limit'))
                ])
    else:
        return f'Error retrieving sls data from BLAZE -- response code: {response.status_code}:{response.reason}'

def insert_to_gsheet(ws: str, df: pd.DataFrame) -> None:
    """
    Insert data to Google sheet
    https://docs.google.com/spreadsheets/d/1On64UQPTGt4qjmNdPnqg32u8ASDPN5iopCRNQ4tkrIg/edit?usp=sharing
    Separates each category to different tabs and includes an "All" tab that gives
    all product information without breakout.
    Args:
        ws (str): worksheet
        df (pd.DataFrame): DF to be written
    """
    wbs = {'inventory': 'BLAZE Inventory', 'sales': 'BLAZE Sales'}
    spread = Spread(wbs[ws])
    if ws == 'inventory':
        spread.df_to_sheet(df, replace=True, index=False, sheet='All')
        spread.df_to_sheet(
            df.loc[df['category_name'] == 'Flower'],
            replace=True,
            index=False,
            sheet='Flower'
            )
        spread.df_to_sheet(
            df.loc[df['category_name'] == 'Oil Cartridges'],
            replace=True,
            index=False,
            sheet='Oil Cartridges'
            )
        spread.df_to_sheet(
            df.loc[
                (df['category_name'] == 'Concentrates') | (df['category_name'] == 'Extracts')
                ],
                replace=True, index=False, sheet='Conc/Extracts'
            )
        spread.df_to_sheet(
            df.loc[df['category_name'] == 'Pre-rolls'],
            replace=True,
            index=False,
            sheet='Pre-rolls'
            )
        spread.df_to_sheet(
            df.loc[df['category_name'] == 'Tinctures'],
            replace=True,
            index=False,
            sheet='Tinctures'
            )
        spread.df_to_sheet(
            df.loc[df['category_name'] == 'Edibles'],
            replace=True,
            index=False,
            sheet='Edibles'
            )
        spread.df_to_sheet(
            df.loc[df['category_name'] == 'Topicals'],
            replace=True,
            index=False,
            sheet='Topicals'
            )
    elif ws == 'sales':
        spread.df_to_sheet(
            df,
            replace=False,
            index=False,
            headers=False,
            sheet='Sheet1'
            )

def dt_gen(start: str, end: str, delta: timedelta=timedelta(days=5)) -> str:
    start = datetime.strptime(start, '%m/%d/%Y')
    end = datetime.strptime(end, '%m/%d/%Y')
    curr = start
    while curr < end:
        yield curr.strftime('%m/%d/%Y'), (curr + delta).strftime()
        curr += delta


def main() -> None:
    """
    main
    Returns: None
    """

    batch_qty = get_blz_batch_qty()
    products = get_products()
    vendors = get_vendors()
    brands = get_brands()
    products['category_name'] = products.category.apply(lambda x: x.get('name'))
    products.loc[
        products['category_name'] == 'Flower', 'unitPrice'
        ] = products.loc[
            products['category_name'] == 'Flower', 'priceRanges'
            ].apply(lambda x: x[1].get('price'))
    inv_summ = products.merge(
        batch_qty,
        how='left',
        right_on='productId',
        left_on='id'
        ).groupby([
            'brandId', 'vendorId', 'productId',
            'sku', 'category_name', 'name', 'unitPrice'
            ]).agg({'quantity': 'sum'}).reset_index().merge(
                vendors,
                how='left',
                left_on='vendorId',
                right_on='id'
            ).merge(
                brands,
                how='left',
                left_on='brandId',
                right_on='brand_id'
            )
    inv_summ = inv_summ.rename(columns={'quantity': 'ohq'})
    inv_summ['onhand($)'] = inv_summ['ohq'] * inv_summ['unitPrice']

    insert_to_gsheet('inventory', df=inv_summ)

    # Insert sales to Google CSV

    sls_file = '/home/ted/Downloads/All Sales Report (8).csv'
    sls = pd.read_csv(sls_file, skiprows=1)
    sls.Date = pd.to_datetime(sls.Date)
    sls = sls.drop_duplicates(
        ['Date', 'Trans No.', 'Product SKU', 'Member', 'Quantity Sold']
        )
    sls['Units'] = sls['Quantity Sold'].str.split(' ', expand=True)[1]
    sls['Quantity Sold'] = sls['Quantity Sold'].str.split(' ', expand=True)[0]
    sls['COGs'] = sls['COGs'].str.strip('$')
    sls['Retail Value'] = sls['Retail Value'].str.strip('$')
    sls['Net Sales'] = sls['Net Sales'].str.strip('$')
    sls = sls.astype({
        'COGs': 'float', 
        'Net Sales': 'float',
        'Retail Value': 'float',
        'Quantity Sold': 'float'
    })

    sls['Unit Cost'] = sls['COGs']/ sls['Quantity Sold']
    sls['Unit Retail'] = sls['Retail Value'] / sls['Quantity Sold']
    sls['Unit Net Sales'] = sls['Net Sales']/ sls['Quantity Sold']

    #TODO fix this to insert to the proper gsheet BLAZE Sales
    df = sls[[
            'Date', 'Trans No.', 'Trans Status',
            'Product SKU', 'Product Name',
            'Product Category', 'Brand Name', 'Vendor',
            'Quantity Sold', 'Units', 'Batch', 'Unit Cost',
            'COGs', 'Retail Value', 'Unit Retail', 'Unit Net Sales',
            'Net Sales', 'Subtotal'
            ]]
    return df


    ##########################################################################
    # For getting sales information from get_sls(). This functionality 
    # pulls from BLAZE API, but currently does not align with ground truth of 
    # existing data in BLAZE. This will need to be reviewed, refactored, and
    # verified against existing data. Going forward, I will instead manually
    # add sales for sourcing reporting.
    ##########################################################################
    # start = (datetime.today() - timedelta(days=5)).strftime('%m/%d/%Y')
    # end = datetime.today().strftime('%m/%d/%Y')
    # for i, j in dt_gen(start=start, end=end):
    #     x = get_sls(start=i, end=j)
    #     xp = x.merge(
    #         p[['id', 'name']],
    #         how='left',
    #         left_on='productId',
    #         right_on='id'
    #         )
if __name__ == '__main__':
    sls = main()
    b = get_blz_batch_qty()
    sls.to_csv('/run/user/1000/gvfs/google-drive:host=gmail.com,user=ted.kushcart/ln_itm_sls_2021.csv')
