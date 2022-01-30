# -*- coding: utf-8 -*-
from data_tools.test_data_looop_handlers import *



if __name__ == "__main__":

    loop_params = {
        'date_start': '2021-12-01',
        'date_end': '2021-12-01',

        'symbol': '1INCHUSDT',

        'minutes_to_trade_after_buy': 120,

        'avg_price_growth_per_step_limit_pct': 0.5,

        'min_price_fall_per_step_limit_pct': 18,
        'max_price_growth_per_step_limit_pct': 25,

        'back_check_period_length': 29,
    }

    path_for_logs = create_folder_for_logs(loop_params)

    # df = get_loop_df(loop_params['date_start'], loop_params['date_end'], loop_params['symbol'], pkl_path='tmp.pkl')
    df = get_loop_df(loop_params['date_start'], loop_params['date_end'], loop_params['symbol'])

    for index, row in df.iterrows():

        current_ts, current_symbol = row['timeOpen'], row['symbol']

        point_info = get_current_point_info(loop_params, index, df)

        if not point_info:
            continue

        low_prices_back_list, low_prices_ahead_list, avg_prices_ahead_list, usd_volumes_back_list = point_info

        if not is_price_growth_above_the_limit(loop_params, low_prices_back_list):
            continue

        if not is_under_the_line(low_prices_back_list):
            print('1')
            continue

        if is_stand_fall_growth_limits(loop_params, low_prices_back_list):
            print('2')
            continue


        trade_result = 'win'
        trade_gain_result_value_pct = 5

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
            trade_gain_result_value_pct
        )


        print('tic')



