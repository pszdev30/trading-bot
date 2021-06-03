import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from decimal import Decimal
import boto3
import aws_secrets

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
secrets = aws_secrets.get_secrets()
screened_tickers_byte = s3.get_object(Bucket='trading-bot-s3',
                                      Key='screened_tickers.json')
screened_tickers_str = screened_tickers_byte['Body'].read().decode('UTF-8')

APCA_API_KEY_ID = secrets["APCA-API-KEY-ID"]
APCA_API_SECRET_KEY = secrets["APCA-API-SECRET-KEY"]
APCA_DATA_BARS_URL = secrets['APCA-DATA-BARS-V1-URL']
HEADERS = json.loads(secrets["HEADERS"])
HEADERS['APCA-API-KEY-ID'] = APCA_API_KEY_ID
HEADERS['APCA-API-SECRET-KEY'] = APCA_API_SECRET_KEY


def insert_data_dynamodb():
    transformed_data = transform()
    bta_data = dynamodb.Table('BTA_Data')
    for key in transformed_data:
        for bar in transformed_data[key]:
            # print(bar)
            entry = bta_data.put_item(
                Item={
                    'ticker': key,
                    'date': bar['date'],
                    'data': {
                        'h': bar['h'],
                        'c': bar['c'],
                        'l': bar['l'],
                        'v': bar['v'],
                        'i': bar['i']
                    }
                })


def transform():
    transformed_data = {}
    data = get_alpaca_price_data()
    for key in data:
        transformed_data[key] = []
        for bar in data[key]:
            time = datetime.fromtimestamp(bar['t'])
            date = time.strftime('%Y-%m-%d')
            transformed_data[key].append({
                'date': date,
                'o': Decimal(str(bar['o'])),
                'h': Decimal(str(bar['h'])),
                'l': Decimal(str(bar['l'])),
                'c': Decimal(str(bar['c'])),
                'v': Decimal(str(bar['v'])),
                'i': Decimal('0.00')
            })
            # print(bar)
    return transformed_data


def get_alpaca_price_data():
    two_hundred_days_ago = (datetime.now() - timedelta(days=200)).date()
    start = pd.Timestamp(two_hundred_days_ago,
                         tz="America/New_York").isoformat()
    end = "now"
    timeframe = "1D"

    symbols = screened_tickers_str
    limit = 200

    DAY_BAR_URL = APCA_DATA_BARS_URL + "/{}?symbols={}&limit={}".format(
        timeframe, symbols, limit)

    r = requests.get(DAY_BAR_URL, headers=HEADERS)
    data = r.json()

    return data


if __name__ == '__main__':
    insert_data_dynamodb()