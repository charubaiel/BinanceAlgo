import requests
import os
import json
from time import sleep
from datetime import datetime

# https://www.binance.com/en/trade/TRXDOWN_USDT?layout=pro&theme=dark

url = 'https://api.binance.com/api/v3/ticker/bookTicker'
response = requests.get(url = url)
assets_raw = json.loads(response.text)
bot_token = os.getenv('baintbot')

best_gain = (None, 0)
while True:

    for coin in assets_raw:
        if 'USDT' in coin['symbol'] and not any(f in coin['symbol'] for f in ['BCHSVUSDT', 'UP', 'DOWN']):
            # print(coin['symbol'])

            # ask_usd_vol = float(coin['askPrice']) * float(coin['askQty'])

            url = f'https://api.binance.com/api/v3/klines?symbol={coin["symbol"]}&interval=1m&limit=3'
            response = requests.get(url = url)
            response_json = json.loads(response.text)[::-1][1:]


            ts = float(response_json[0][0])
            from time import time
            if ts < (int(time()) - int(time()) % 60 - 60 * 2) * 1000:
                continue
            ts_end = float(response_json[0][6])
            # ts = datetime.utcfromtimestamp(str(int(ts))).strftime('%Y-%m-%d %H:%M:%S')
            close_cost_curr = float(response_json[0][4])
            close_cost_past = float(response_json[1][4])
            quote_asset_volume = float(response_json[0][7])
            quote_asset_volume_past = float(response_json[1][7])
            number_of_trades = float(response_json[0][8])
            number_of_trades_past = float(response_json[1][8])

            gain = round(100.*((close_cost_curr / close_cost_past) - 1), 2)
            usd_vol_increase = round(100.*((quote_asset_volume / quote_asset_volume_past) - 1), 2) if quote_asset_volume_past > 0 else 0
            number_of_trades_increase = round(100.*((number_of_trades / number_of_trades_past) - 1), 2) if number_of_trades_past > 0 else 0



            ts = datetime.utcfromtimestamp(int(ts/1000)).strftime('%Y-%m-%d %H:%M:%S')

            ts_end = datetime.utcfromtimestamp(int(ts_end/1000)).strftime('%Y-%m-%d %H:%M:%S')
            symbol = coin['symbol']
            if gain <= -3 or gain >= 3 or (usd_vol_increase >= 1000 and quote_asset_volume >= 100_000):
                msg = f"Binance minute monitoring\n\nSymbol: {symbol}\nCost growth: {gain=}%\n" \
                      f"Volume: {quote_asset_volume} USDT ({usd_vol_increase}%)\n" \
                      f"Number of trades: {number_of_trades} ({number_of_trades_increase}%)\n" \
                      f"UTC start: {ts}\nUTC end: {ts_end=}\n\n" \
                      f"https://www.binance.com/en/trade/{coin['symbol'].replace('USDT', '_USDT')}" \
                      f"?layout=pro&theme=dark"
                tg_url = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id=-1001706274303&text={msg}'
                requests.get(url = tg_url)
            if gain >= best_gain[1]:
                best_gain = (coin['symbol'], gain)
            print(f"{best_gain=} |> {symbol=}, {gain=}, {ts=}, {ts_end=}")

