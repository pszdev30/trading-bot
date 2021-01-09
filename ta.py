import alpaca_trade_api as alpaca
import pandas as pd
import json
from secrets import *
from globals import *
import requests


api = alpaca.REST(APCA_API_KEY_ID, APCA_API_SECRET_KEY,
                  APCA_API_PORTFOLIO_BASE_URL)

timeframe = '1D'
symbols = ','.join(screened_tickers)
limit = 1

DAY_BAR_URL = APCA_DATA_BARS_URL + \
    '/{}?symbols={}&limit={}'.format(timeframe, symbols, limit)


r = requests.get(DAY_BAR_URL, headers=HEADERS)

print(json.dumps(r.json(), indent=4))
