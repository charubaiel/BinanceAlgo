# -*- coding: utf-8 -*-
from datetime import timedelta
import yaml
from tools.data_loop_handlers import *
from tools.data_loop_filters import *
from tools.color_loger import *


if __name__ == "__main__":

    with open('config.yaml', 'r', encoding='utf8') as f:
        loop_params = yaml.safe_load(f)['loop_params']

    path_for_logs = create_folder_for_logs(loop_params, log, formatter)

    df = get_loop_df(
        loop_params['date_start'],
        loop_params['date_end'],
        loop_params['symbol']
        # , use_caсhe=True
    )

    prev_ts, prev_symbol, prev_trade_deadline = '', '', ''
    winning_trades_cnt, losing_trades_cnt,  outoftime_trades_cnt = 0, 0, 0
    usd_balance = loop_params['usd_balance']

    for index, row in df.iterrows():

        point_info = get_current_point_info(loop_params, index, df)

        if not point_info:
            continue

        low_prices_back_list, low_prices_ahead_list, avg_prices_ahead_list, usd_volumes_back_list = point_info

        if not is_price_growth_above_the_limit(loop_params, low_prices_back_list):
            continue

        if not is_under_the_line(low_prices_back_list):
            # print('not is_under_the_line')
            continue

        if not is_enough_volume(usd_volumes_back_list):
            # print('not is_enough_volume')
            continue

        current_ts, current_symbol = row['timeOpen'], row['symbol']

        trade_deadline = (
                datetime.strptime(
                    current_ts,
                    '%Y-%m-%d %H:%M:%S'
                ) + timedelta(minutes=loop_params['minutes_to_trade_after_buy'])
        ).strftime('%Y-%m-%d %H:%M:%S')

        if current_symbol == prev_symbol and current_ts <= prev_trade_deadline:
            continue

        if not is_stand_fall_growth_limits_original(loop_params, low_prices_back_list):
            # print('not is_stand_fall_growth_limits')
            continue

        usd_balance, trade_result, winning_trades_cnt, losing_trades_cnt, outoftime_trades_cnt, trade_profit_pct = get_trade_result(
            loop_params,
            low_prices_ahead_list,
            avg_prices_ahead_list,
            usd_balance,
            winning_trades_cnt,
            losing_trades_cnt,
            outoftime_trades_cnt,
            row['symbol'],
            row['timeOpen']
        )

        drow_point_info_png(
            index,
            loop_params,
            path_for_logs,
            df,
            low_prices_back_list,
            avg_prices_ahead_list,
            current_symbol,
            current_ts,
            trade_result,
            trade_profit_pct
        )

        prev_ts, prev_symbol, prev_trade_deadline = current_ts, current_symbol, trade_deadline



