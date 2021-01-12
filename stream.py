import alpaca_trade_api as alpaca
import websocket
import json
import pandas as pd
from secrets import *
import ta


# Alpaca API config stuff

AUTHENTICATE = {
    "action": "authenticate",
    "data": {
        "key_id": APCA_API_KEY_ID,
        "secret_key": APCA_API_SECRET_KEY
    }
}

tickers = ['alpacadatav1/AM.MSFT', 'alpacadatav1/AM.FB']

listen = {
    'action': 'listen', 'data': {'streams': tickers}
}


def on_open(ws):
    ws.send(json.dumps(AUTHENTICATE))
    ws.send(json.dumps(listen))


def on_message(ws, minute_bar):

    ta.ingest_stream(minute_bar)


def on_close(ws):
    print('closed connection')


socket = APCA_WEB_SOCKET

ws = websocket.WebSocketApp(socket, on_open=on_open,
                            on_message=on_message, on_close=on_close)
ws.run_forever()
