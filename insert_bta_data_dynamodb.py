import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from decimal import Decimal
import aws_secrets
import boto3
from boto3.dynamodb.conditions import Attr
import botocore

dynamodb = boto3.resource('dynamodb')


def create_table():
    bta_data = dynamodb.create_table(TableName='BTA-Data',
                                     KeySchema=[{
                                         'AttributeName': 'ticker',
                                         'KeyType': 'HASH'
                                     }, {
                                         'AttributeName': 'date',
                                         'KeyType': 'RANGE'
                                     }],
                                     AttributeDefinitions=[{
                                         'AttributeName': 'ticker',
                                         'AttributeType': 'S'
                                     }, {
                                         'AttributeName': 'date',
                                         'AttributeType': 'S'
                                     }],
                                     BillingMode='PAY_PER_REQUEST')
    print('BTA-Data status: ', bta_data.table_status)


s3 = boto3.client('s3')
secrets = aws_secrets.get_secrets()
screened_tickers_byte = s3.get_object(Bucket='trading-bot-s3',
                                      Key='screened_tickers.json')
screened_tickers_str = screened_tickers_byte['Body'].read().decode('UTF-8')
screened_tickers_list = (screened_tickers_str).split(',')

APCA_API_KEY_ID = secrets["APCA-API-KEY-ID"]
APCA_API_SECRET_KEY = secrets["APCA-API-SECRET-KEY"]
APCA_DATA_BARS_URL = secrets['APCA-DATA-BARS-V1-URL']
HEADERS = json.loads(secrets["HEADERS"])
HEADERS['APCA-API-KEY-ID'] = APCA_API_KEY_ID
HEADERS['APCA-API-SECRET-KEY'] = APCA_API_SECRET_KEY
REDIS_ELASTICACHE_CLUSTER_URL = secrets['REDIS-ELASTICACHE-CLUSTER-URL']


def insert_data_dynamodb():
    transformed_data = transform()
    bta_data = dynamodb.Table('BTA-Data')
    with bta_data.batch_writer() as batch:
        for key in transformed_data:
            for bar in transformed_data[key]:
                entry = batch.put_item(
                    Item={
                        'ticker': key,
                        'date': bar['date'],
                        'data': {
                            'o': bar['o'],
                            'h': bar['h'],
                            'c': bar['c'],
                            'l': bar['l'],
                            'v': bar['v'],
                            'i': bar['i']
                        }
                    })
            print('Completed ticker', key)


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


def get_alpaca_price_data():
    two_hundred_days_ago = (datetime.now() - timedelta(days=200)).date()
    start = pd.Timestamp(two_hundred_days_ago,
                         tz='America/New_York').isoformat()
    end = 'now'
    timeframe = '1D'

    symbols = screened_tickers_str
    limit = 200

    DAY_BAR_URL = APCA_DATA_BARS_URL + '/{}?symbols={}&limit={}'.format(
        timeframe, symbols, limit)

    r = requests.get(DAY_BAR_URL, headers=HEADERS)
    data = r.json()

    return data


def update_bta_data():
    bta_data = dynamodb.Table('BTA-Data')
    transformed_data = transform()
    with bta_data.batch_writer() as batch:
        for key in transformed_data:
            for bar in transformed_data[key]:
                date_time = datetime.fromtimestamp(bar['t'])
                date = date_time.strftime('%Y-%m-%d').split(' ')[0]
                try:
                    update = batch.update_item(
                        Key={
                            'ticker': key,
                            'date': date
                        },
                        UpdateExpression='SET #attrName.o = :open',
                        ExpressionAttributeNames={'#attrName': 'data'},
                        ExpressionAttributeValues={':open': Decimal(bar['o'])},
                        ConditionExpression=Attr('#attrName.o').not_exists())
                except botocore.exceptions.ClientError as e:
                    if (e.response['Error']['Code'] ==
                            'ConditionalCheckFailedException'):
                        raise
            print('completed ticker:', key)
        print('Completed update!')


def delete_old_data():
    bta_data = dynamodb.Table('BTA-Data')
    with bta_data.batch_writer() as batch:
        for ticker in screened_tickers_list:
            batch.delete_item(Key={'ticker': ticker, 'date': '2020-08-17'})
            batch.delete_item(Key={'ticker': ticker, 'date': '2020-08-18'})


def reset_data():
    bta_data = dynamodb.Table('BTA-Data')
    transformed_data = transform()
    with bta_data.batch_writer() as batch:
        for key in transformed_data:
            for bar in transformed_data[key]:
                batch.delete_item(Key={'ticker': key, 'date': bar['date']})
            print('finished reset:', key)


def reinsert_modified_format():
    bta_data = dynamodb.Table('BTA-Data')
    transformed_data = transform()
    with bta_data.batch_writer() as batch:
        for key in transformed_data:
            for bar in transformed_data[key]:
                batch.put_item(
                    Item={
                        'ticker': key,
                        'date': bar['date'],
                        'o': bar['o'],
                        'h': bar['h'],
                        'c': bar['c'],
                        'l': bar['l'],
                        'v': bar['v'],
                        'i': bar['i']
                    })
            print('finished reinsertion:', key)


if __name__ == '__main__':
    reset_data()
    reinsert_modified_format()
    # delete_old_data()
    # update_bta_data()
    # date_time = datetime.fromtimestamp(1545730073)
    # date = date_time.strftime('%Y-%m-%d').split(' ')[0]
    # print(date)
# create_table()
# insert_data_dynamodb()
