import alpaca_trade_api as alpaca
import websocket
import json
import pandas as pd
from secrets import *


# Alpaca API config stuff


# real-time API for alpaca (if needed)
# api = alpaca.REST(APCA_API_KEY, APCA_SECRET_KEY, APCA_API_BASE_URL)


AUTHENTICATE = {
    "action": "authenticate",
    "data": {
        "key_id": APCA_API_KEY,
        "secret_key": APCA_SECRET_KEY
    }
}

listen = {
    'action': 'listen', 'data': {'streams': ['Q.MSFT', 'Q.FB', 'Q.CMCSA', 'Q.KO']}
}


def on_open(ws):
    ws.send(json.dumps(AUTHENTICATE))
    ws.send(json.dumps(listen))


def on_message(ws, message):
    print(message)


def on_close(ws):
    pass


socket = 'wss://data.alpaca.markets/stream'

ws = websocket.WebSocketApp(socket, on_open=on_open,
                            on_message=on_message, on_close=on_close)
ws.run_forever()
