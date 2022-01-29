# -*- coding: utf-8 -*-
import pandas as pd
from data_tools.color_loger import log
from data_tools.clickhouse_handlers import ch_select





def get_loop_df(date_start: str, date_end: str, symbol: str = '') -> pd.DataFrame:
    query = f"select" \
            f"     symbol," \
            f"     timeOpen," \
            f"     priceUsd_open," \
            f"     priceUsd_low," \
            f"     priceUsd_weighted" \
            f"from default.binanceCandle1m" \
            f"where    1=1" \
            f"     and toDate(timeOpen) between '{date_start}' and '{date_end}'" \
            f"     and sympol ilike '%{symbol}%'" \
            f"order by" \
            f"     symbol, timeOpen asc"

    df = ch_select(query, 'df')

    # df.to_pickle('tmp.pkl')

    ttl_rows_in_df = df.shape[0]

    log.info(f'df for the received. {ttl_rows_in_df=}')

    return df


if __name__ == "__main__":
    print(get_loop_df('2022-01-01', '2022-01-01', 'BTCUSDT').to_string())
