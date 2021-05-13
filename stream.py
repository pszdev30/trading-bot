from alpaca_trade_api import StreamConn
from alpaca_trade_api.common import URL
import websocket
import json
import pandas as pd
from globals import *
from secrets import *
import create_signal
import asyncio
import logging
import threading
import time
import schedule
from alpaca_trade_api.stream import Stream


# Alpaca API config stuff

async def bar_callback(minute_bar):
    print('bar', minute_bar)
    create_signal.ingest_stream(minute_bar)


schedule.every().day.at('9:15').do(start_job())


def start_job():
    # Initiate Class Instance
    stream = Stream(APCA_API_KEY_ID,
                    APCA_API_SECRET_KEY,
                    base_url=URL(APCA_API_PORTFOLIO_BASE_URL),
                    data_feed='iex')  # <- replace to SIP if you have PRO subscription

    # subscribing to event
    stream.subscribe_bars(bar_callback, 'PYPL')
    stream.run()
