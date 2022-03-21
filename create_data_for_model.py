# -*- coding: utf-8 -*-
from datetime import timedelta, datetime
import yaml
from tools.data_loop_handlers import *
from tools.data_loop_filters import *
from tools.color_loger import *
from tools.clickhouse_handlers import *


if __name__ == "__main__":

    with open('config.yaml', 'r', encoding='utf8') as f:
        loop_params = yaml.safe_load(f)['loop_params']


    path_for_logs = create_folder_for_logs(loop_params, log, formatter)

    for symbol in loop_params['symbol']:

        df = get_loop_df(
            'raw_olh_1m_coins_large',
            loop_params['date_start'],
            loop_params['date_end'],
            loop_params['symbol'],
            # use_cache=True
        )

        prev_ts, prev_symbol, prev_trade_deadline = '', '', ''
        winning_trades_cnt, losing_trades_cnt, outoftime_trades_cnt = 0, 0, 0
        usd_balance = loop_params['usd_balance']

        for index, row in df.iterrows():

            point_info = get_current_large_point_info(loop_params, index, df)

            if not point_info:
                continue

            if not is_price_growth_above_the_limit(loop_params, point_info['low_prices_back_list']):
                continue

            if not is_under_the_line(point_info['low_prices_back_list']):
                under_the_line = 0
            else:
                under_the_line = 1

            if not is_enough_volume(point_info['ttl_vol_large_back_list'][-29:]):
                enough_volume = 0
            else:
                enough_volume = 1

            if not is_stand_fall_growth_limits_original(loop_params, point_info['low_prices_back_list']):
                stand_fall_growth_limits_original = 0
            else:
                stand_fall_growth_limits_original = 1

            current_ts, current_symbol = row['timeOpen'], row['symbol']

            trade_deadline = (
                    datetime.strptime(
                        current_ts,
                        '%Y-%m-%d %H:%M:%S'
                    ) + timedelta(minutes=loop_params['minutes_to_trade_after_buy'])
            ).strftime('%Y-%m-%d %H:%M:%S')

            if current_symbol == prev_symbol and current_ts <= prev_trade_deadline:
                continue

            row_to_insert = [[
                current_symbol,
                current_ts,
                list(point_info['low_prices_back_list']),
                list(point_info['low_prices_ahead_list']),
                list(point_info['high_prices_back_list']),
                list(point_info['high_prices_ahead_list']),
                list(point_info['open_prices_back_list']),
                list(point_info['open_prices_ahead_list']),
                list(point_info['avg_prices_large_back_list']),
                list(point_info['avg_prices_ahead_list']),
                list(point_info['ttl_tsell_vol_back_list']),
                list(point_info['ttl_vol_large_back_list']),
                under_the_line,
                enough_volume,
                stand_fall_growth_limits_original,
                0
            ], ]


            ch_insert(row_to_insert, 'binanceFastGrowth29m')

            # drow_point_info_png(
            #     loop_params,
            #     path_for_logs,
            #     point_info,
            #     current_symbol,
            #     current_ts
            # )

            prev_ts, prev_symbol, prev_trade_deadline = current_ts, current_symbol, trade_deadline

