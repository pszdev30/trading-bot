from alpaca_trade_api.rest import REST
from alpaca_trade_api.common import URL
import websocket
import json
import threading
import time
import secrets
import globals
import ssl
import boto3

api = REST(secrets.APCA_API_KEY_ID, secrets.APCA_API_SECRET_KEY,
           secrets.APCA_API_PORTFOLIO_BASE_URL)

# Alpaca WebSocket config stuff

AUTH = {
    "action": "auth",
    "key": secrets.APCA_API_KEY_ID,
    "secret": secrets.APCA_API_SECRET_KEY
}

LISTEN = {"action": "subscribe", "bars": globals.screened_tickers}


def on_open(ws):
    ws.send(json.dumps(AUTH))
    ws.send(json.dumps(LISTEN))


def on_message(ws, minute_bar):
    region = 'us-east-2'
    instance = 'i-072a90a706d9e42d9'
    # command = 'python3 trading-bot-scripts/create_signal.py {minute_bar}'.format(
    #     minute_bar=minute_bar)
    # trade_ec2 = boto3.client('ssm', region_name=region)
    # trade_ec2.send_command(
    #     InstanceIds=[
    #         instance
    #     ],
    #     Comment='Triggers trading-bot-main',
    #     DocumentName='AWS-RunShellScript',
    #     Parameters={
    #         'commands': [
    #             command
    #         ]
    #     }
    # )
    # print(minute_bar)
    # create_signal.start(minute_bar)


def on_close(ws):
    print('Trading closed for the day :)')


def on_error(ws, error_msg):
    print(error_msg)


def get_time_to_market_close():
    clock = api.get_clock()
    return (clock.next_close - clock.timestamp).total_seconds()


def start_socket_connection():
    # websocket.enableTrace(True)
    socket = secrets.APCA_WEB_SOCKET
    ws = websocket.WebSocketApp(socket,
                                on_open=on_open,
                                on_message=on_message,
                                on_close=on_close,
                                on_error=on_error)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

    # thread = threading.Thread(target=ws.run_forever)
    # thread.start()
    # time.sleep(round(get_time_to_market_close()) + 300)
    # ws.keep_running = False
    # thread.join()
    # print('Trading closed for the day :)')


start_socket_connection()
