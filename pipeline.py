# import alpaca_trade_api as alpaca
from alpaca_trade_api.rest import REST
from datetime import datetime, timedelta
from decimal import Decimal
import pandas as pd
import requests
import json
import aws_secrets
from boto3.dynamodb.conditions import Key
import boto3

dynamodb = boto3.resource('dynamodb')

secrets = aws_secrets.get_secrets('APCA-SECRETS')
APCA_API_KEY_ID = secrets["APCA-API-KEY-ID"]
APCA_API_SECRET_KEY = secrets["APCA-API-SECRET-KEY"]
APCA_API_PORTFOLIO_BASE_URL = secrets["APCA-API-PORTFOLIO-BASE-URL"]
APCA_DATA_BARS_URL = secrets["APCA-DATA-BARS-V1-URL"]
HEADERS = json.loads(secrets["HEADERS"])
HEADERS['APCA-API-KEY-ID'] = APCA_API_KEY_ID
HEADERS['APCA-API-SECRET-KEY'] = APCA_API_SECRET_KEY

api = REST(APCA_API_KEY_ID, APCA_API_SECRET_KEY, APCA_API_PORTFOLIO_BASE_URL)

screened_tickers_byte = s3_client.get_object(Bucket='trading-bot-s3',
                                             Key='screened_tickers.json')
screened_tickers_str = screened_tickers_byte['Body'].read().decode('UTF-8')
screened_tickers_list = (screened_tickers_str).split(',')


def get_screened_ticker_data():
    two_hundred_days_ago = (datetime.now() - timedelta(days=200)).date()
    start = pd.Timestamp(two_hundred_days_ago,
                         tz='America/New_York').isoformat()
    end = 'now'
    timeframe = '1D'

    symbols = screened_tickers_str
    limit = 1

    DAY_BAR_URL = APCA_DATA_BARS_URL + "/{}?symbols={}&limit={}".format(
        timeframe, symbols, limit)

    r = requests.get(DAY_BAR_URL, headers=HEADERS)
    data = r.json()

    return data


def transform():
    transformed_data = {}
    data = get_screened_ticker_data()
    for key in data:
        transformed_data[key] = []
        for bar in data[key]:
            time = datetime.fromtimestamp(bar['t'])
            date = time.strftime('%Y-%m-%d')
            transformed_data[key].append({
                'date': date,
                't': Decimal(str(bar['t'])),
                'o': Decimal(str(bar['o'])),
                'h': Decimal(str(bar['h'])),
                'l': Decimal(str(bar['l'])),
                'c': Decimal(str(bar['c'])),
                'v': Decimal(str(bar['v'])),
                'i': Decimal('0.00')
            })
            # print(bar)
    return transformed_data


def update_bta_data():
    transformed_data = transform()
    bta_data = dynamodb.Table('BTA-Data')
    with bta_data.batch_writer() as batch:
        for key in transformed_data:
            for bar in transformed_data[key]:
                try:
                    bta_data.put_item(
                        Item={
                            'ticker': key,
                            'date': bar['date'],
                            'o': bar['o'],
                            'h': bar['h'],
                            'l': bar['l'],
                            'c': bar['c'],
                            'v': bar['v'],
                            'i': bar['i'],
                            'ttl': 17366400
                        })
                except botocore.exceptions.ClientError as e:
                    print('Exception:', e)
                    raise
            print('update completed:', key)
        print('daily price data loaded!')


def retrieve_bta_data():
    pass


def load_redis_cache():
    pass


update_bta_data()
retrieve_bta_data()
load_redis_cache()

# with open('output.txt', 'w') as output:
#     output.write(data)
# print(json.dumps(data['MSFT']['t'], indent=4))
