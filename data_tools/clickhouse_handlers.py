# -*- coding: utf-8 -*-
import json
from io import BytesIO
import pandas as pd
from time import sleep
import os
import requests
from color_loger import log


def try_5_times(func, tries = 5, delay = 3):
    def wraper(*args, **kwargs):
        for t in range(tries):
            try:
                result = func(*args, **kwargs)

                return result

            except Exception as e:
                if tries - t != 1:
                    log.warning(f'attempt {t + 1}/{tries} failed, sleep {delay} seconds. Error: \n{e}')

                    sleep(delay)
                else:
                    raise

    return wraper


@try_5_times
def bn_get_candle(symbol: str, ts_start: str, ts_end: str) -> list:
    url = f'https://api.binance.com/api/v3/klines?' \
          f'symbol={symbol}&interval=1m&limit=1000&' \
          f'startTime={ts_start}&' \
          f'endTime={ts_end}'

    response = requests.get(url = url)

    candle_data = json.loads(response.text)

    if candle_data or candle_data == []:
        return candle_data
    else:
        raise ValueError(f'Empty data: {url}')


@try_5_times
def ch_insert(data: list, table: str) -> None:
    q = requests.utils.requote_uri(f'insert into {table} FORMAT TSV')

    d = ('\n'.join(['\t'.join(map(str, r)) for r in data])).encode('utf-8')

    out = requests.post(url = f"{os.getenv('ch_url')}?query={q}", data = d, auth = ('ch', os.getenv('ch_ch')))

    if out.status_code == 200 and data:
        return

    if out.status_code == 500 and 'not enough space' in out.text:
        requests.post(url = f"{os.getenv('ch_url')}", data = f'OPTIMIZE TABLE {table} FINAL'.encode('utf-8'),
                      auth = ('ch', os.getenv('ch_ch')))

        log.info('sleeped for 30 sec')
        sleep(30)

    else:

        raise ValueError(out.status_code, out.text)


@try_5_times
def ch_select(query: str) -> pd.DataFrame:
    ch_format = 'CSVWithNames' if format == 'df' else 'JSONCompact'

    data = (query + f'FORMAT {ch_format}').encode('utf-8')

    out = requests.post(url = f"{os.getenv('ch_url')}", data = data, auth = ('ch', os.getenv('ch_ch')))

    if out.status_code == 200 and out.text != '':

        return pd.read_csv(BytesIO(out.content))

    else:

        raise Exception(f'\nStatus: {out.status_code}\n{out.text}')
