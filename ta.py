import alpaca_trade_api as alpaca
import pandas as pd
import json
from secrets import *


api = alpaca.REST(APCA_API_KEY, APCA_SECRET_KEY, APCA_API_BASE_URL)
