# import alpaca_trade_api as alpaca
from alpaca_trade_api.rest import REST
from datetime import datetime, timedelta
from decimal import Decimal
import pandas as pd
import requests
import json
import time
from direct_redis import DirectRedis
import aws_secrets
import btalib
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
import boto3
import botocore

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

secrets = aws_secrets.get_secrets('APCA-SECRETS')
aws_services_secrets = aws_secrets.get_secrets(
    'TRADING-BOT-AWS-SERVICES-SECRETS')
APCA_API_KEY_ID = secrets["APCA-API-KEY-ID"]
APCA_API_SECRET_KEY = secrets["APCA-API-SECRET-KEY"]
APCA_API_PORTFOLIO_BASE_URL = secrets["APCA-API-PORTFOLIO-BASE-URL"]
APCA_DATA_BARS_URL = secrets["APCA-DATA-BARS-V1-URL"]
HEADERS = json.loads(secrets["HEADERS"])
HEADERS['APCA-API-KEY-ID'] = APCA_API_KEY_ID
HEADERS['APCA-API-SECRET-KEY'] = APCA_API_SECRET_KEY
REDIS_ELASTICACHE_CLUSTER_URL = aws_services_secrets[
    'REDIS-ELASTICACHE-CLUSTER-URL']

api = REST(APCA_API_KEY_ID, APCA_API_SECRET_KEY, APCA_API_PORTFOLIO_BASE_URL)

screened_tickers_byte = s3.get_object(Bucket='trading-bot-s3',
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


def retrieve_bta_data(ticker):
    bta_data = dynamodb.Table('BTA-Data')
    current_epoch = int(time.time())
    try:
        ticker_data = bta_data.query(
            KeyConditionExpression='#ticker = :symbol',
            FilterExpression='#ttl > :current_epoch',
            ExpressionAttributeNames={
                '#ticker': 'ticker',
                '#ttl': 'ttl'
            },
            ExpressionAttributeValues={
                ':symbol': ticker,
                ':current_epoch': current_epoch
            })

        if ('Items' in ticker_data and len(ticker_data['Items']) >= 1):
            return ticker_data['Items']
        return None
    except Exception as e:
        print('Exception:', e)


def load_redis_cache():
    redis_cache = DirectRedis.from_url(REDIS_ELASTICACHE_CLUSTER_URL)
    for ticker in screened_tickers_list:
        ticker_data = retrieve_bta_data(ticker)
        if (ticker_data != None):
            ticker_df = pd.DataFrame.from_dict(ticker_data, orient='columns')
            redis_cache.set('{}_df'.format(ticker), ticker_df)
            redis_cache.set('{}_long_ema'.format(ticker), ema(df=ticker_df))
            redis_cache.set('{}_rsi'.format(ticker), rsi(df=ticker_df))
            redis_cache.set('{}_mfi'.format(ticker), mfi(df=ticker_df))
            redis_cache.lpush('{}_macd'.format(ticker), macd(df=ticker_df))
            redis_cache.rpush('{}_macd'.format(ticker),
                              macd_signal(df=ticker_df))
            expire_datetime = datetime.now() + timedelta(hours=8)
            redis_cache.expireat('{ticker}_df', expire_datetime)
            redis_cache.expireat('{ticker}_long_ema', expire_datetime)
            redis_cache.expireat('{ticker}_rsi', expire_datetime)
            redis_cache.expireat('{ticker}_mfi', expire_datetime)
            redis_cache.expireat('{ticker}_macd', expire_datetime)
            print('finished ticker:', ticker)


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
                            'ttl': int(time.time() + 60 * 60 * 24 * 200)
                        },
                        ConditionExpression=Attr('ticker').ne(key)
                        & Attr('date').ne(bar['date']))
                except botocore.exceptions.ClientError as e:
                    if e.response['Error'][
                            'Code'] != 'ConditionalCheckFailedException':
                        raise
            print('update completed:', key)
        print('daily price data loaded!')


update_bta_data()
# print(REDIS_ELASTICACHE_CLUSTER_URL)
# print(load_redis_cache())


def ema(df):
    return btalib.ema(df, period=200).df.ema.iat[-1]


def rsi(df):
    return btalib.rsi(df, period=200).df.rsi.iat[-1]


def macd(df):
    return str(btalib.macd(df, period=200).df.macd.iat[-1])


def macd_signal(df):
    return str(btalib.macd(df, period=200).df.signal.iat[-1])


def mfi(df):
    return btalib.mfi(df, period=14).df.mfi


# update_bta_data()
# retrieve_bta_data()
# load_redis_cache()

# with open('output.txt', 'w') as output:
#     output.write(data)
# print(json.dumps(data['MSFT']['t'], indent=4))
