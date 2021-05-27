# import alpaca_trade_api as alpaca
from alpaca_trade_api.rest import REST
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import boto3
import aws_secrets

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
secrets = aws_secrets.get_secrets()

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
screened_tickers_str = screened_tickers_byte.get()['Body'].read().decode(
    'UTF-8')
screened_tickers_list = (screened_tickers_str).split(',')


def get_screened_ticker_data():
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


def transform():
    data = get_screened_ticker_data()


def push_to_s3(file, ticker):
    # try:
    #     response = s3_client.upload_file(
    #         file, 'trading-bot-s3',
    #         '/bta-data/{ticker}.txt'.format(ticker=ticker))
    # except ClientError as e:
    #     logging.error(e)
    #     return False
    return True


transform()

# with open('output.txt', 'w') as output:
#     output.write(data)
# print(json.dumps(data['MSFT']['t'], indent=4))
