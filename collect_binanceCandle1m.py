# -*- coding: utf-8 -*-
import json
import requests
from datetime import datetime, timezone
from tools import ch_insert, bn_get_candle
import pandas as pd



url_ticker = 'https://api.binance.com/api/v3/ticker/bookTicker'
response = requests.get(url = url_ticker)
assets_raw = json.loads(response.text)
start = datetime.strptime('2021-11-01', '%Y-%m-%d')
end = datetime.strptime('2022-01-18', '%Y-%m-%d')
eds = pd.date_range(start, end, freq = '12H')



for coin in assets_raw:


    if coin['symbol'].endswith('USDT') and not any(f in coin['symbol'] for f in ['BCHSVUSDT', 'UP', 'DOWN']):
        symbol = coin['symbol']
    else:
        continue


    for s, e in zip(eds[:-1], eds[1:]):
        ts_start = str(int(s.replace(tzinfo = timezone.utc).timestamp() * 1000))
        ts_end = str(int(e.replace(tzinfo = timezone.utc).timestamp() * 1000) - 1000)
        print(symbol, s, e)
        candle_data = bn_get_candle(symbol, ts_start, ts_end)


        data = []
        for candle in candle_data:
            timeOpen = datetime.utcfromtimestamp(candle[0]/1000).strftime('%Y-%m-%d %H:%M:%S')
            timeClose = datetime.utcfromtimestamp(candle[6]/1000).strftime('%Y-%m-%d %H:%M:%S')
            priceUsd_open = candle[1]
            priceUsd_high = candle[2]
            priceUsd_low = candle[3]
            priceUsd_close = candle[4]
            volumeUsd = candle[7]
            volumeCoin = candle[5]
            volumeCoin_whenTakerBuyCoin = candle[9]
            volumeUsd_whenTakerSellCoin = candle[10]
            tradesCnt = candle[8]
            data.append(
                [symbol, timeOpen, timeClose, priceUsd_open, priceUsd_high, priceUsd_low, priceUsd_close,
                 volumeUsd, volumeCoin, volumeCoin_whenTakerBuyCoin, volumeUsd_whenTakerSellCoin, tradesCnt]
            )

            ch_insert(data, 'binanceCandle1m')