# -*- coding: utf-8 -*-
import os
from datetime import datetime
from random import choices
import json
from string import ascii_lowercase
import pandas as pd
import matplotlib.pyplot as plt
from data_tools.color_loger import log
from data_tools.clickhouse_handlers import ch_select


def get_loop_df(date_start: str, date_end: str, symbol: str = '', pkl_path: str = '') -> pd.DataFrame:
    with open('sql/raw_olh_1m_coins.sql', 'r') as f:
        query = f.read()

    query = query.format(date_start=date_start, date_end=date_end, symbol=symbol)

    if pkl_path:
        df = pd.read_pickle(pkl_path)
    else:
        df = ch_select(query)
        df.to_pickle('tmp.pkl')

    ttl_rows_in_df = df.shape[0]

    log.info(f'DataFrame for the loop received. {ttl_rows_in_df=}')

    return df


def create_folder_for_logs(loop_params: dict) -> str:
    run_name = ''.join(choices(ascii_lowercase, k=3))

    run_time = datetime.now().strftime('%Y%m%dT%H%M%S')

    run_folder_name = f'{run_time}_{run_name}'
    run_folder_name = 'test'

    path_for_logs = f'data_test_loop/{run_folder_name}'

    try:
        os.mkdir(path_for_logs)

    except OSError:
        log.info(f'Creation of the directory failed {path_for_logs}')

    else:
        log.info(f'Successfully created the directory {path_for_logs}')

    with open(path_for_logs + '/params.json', 'w') as f:
        json.dump(loop_params, f)

    return path_for_logs


def get_current_point_info(loop_params: dict, index: int, df: pd.DataFrame) -> tuple:
    """
    :param loop_params: Dictionary with scan launch parameters
    :param index: The number of the current line in the raw OLH data
    :param df: DataFrame with timestamp sorted OLH data
    :return: Current point info
    """

    low_prices_back_list = df.iloc[
                               index - loop_params['back_check_period_length']:
                               index, 3
                           ].to_list()

    low_prices_ahead_list = df.iloc[
                                index:
                                index + loop_params['minutes_to_trade_after_buy'],
                                3
                            ].to_list()

    avg_prices_ahead_list = df.iloc[
                                index:
                                index + loop_params['minutes_to_trade_after_buy'],
                                4
                            ].to_list()

    usd_volumes_back_list = df.iloc[
                                index - loop_params['back_check_period_length']:
                                index,
                                5
                            ].to_list()

    symb_list_info_period = df.iloc[
                                index - loop_params['back_check_period_length']:
                                index + loop_params['minutes_to_trade_after_buy'],
                                0
                            ].to_list()

    # пропускаем строчку если в рассматриваемый период попали 2 монеты
    # т.е. не рассматриваем границы между разными монетами
    if len(set(symb_list_info_period)) > 1:
        return ()

    # пропускам строчку, если невозможно забрать полную
    # историю цен, т.е. пропускаем первые строчки
    if index < loop_params['back_check_period_length']:
        return ()

    # пропускаем строчку, если невозможно забрать полный список
    # цен после рассматриваемого момента для проверки возможности продать
    if len(low_prices_ahead_list) != loop_params['minutes_to_trade_after_buy']:
        return ()

    return (
        low_prices_back_list,
        low_prices_ahead_list,
        avg_prices_ahead_list,
        usd_volumes_back_list
    )


def is_under_the_line(low_prices_back_list: list) -> bool:
    """Check if the second half of prices
    list is under the first/last prices line
    """
    x_list = [i for i in range(int(len(low_prices_back_list) / 2), len(low_prices_back_list))]
    y_list = low_prices_back_list[int(len(low_prices_back_list) / 2):]

    k = (low_prices_back_list[-1] - low_prices_back_list[0]) / (len(low_prices_back_list) - 1)
    b = low_prices_back_list[0]

    for x, y in zip(x_list, y_list):
        y0 = k * x + b

        if y > y0:
            return False

    return True


def is_price_growth_above_the_limit(loop_params: dict, low_prices_back_list: list) -> bool:
    avg_growth_period_limit = loop_params['avg_price_growth_per_step_limit_pct']
    period_length = loop_params['back_check_period_length']

    min_growth_for_period_limit = avg_growth_period_limit * period_length
    growth_for_period = (100 * low_prices_back_list[-1] / low_prices_back_list[0]) - 100

    if growth_for_period < min_growth_for_period_limit:
        return False

    return True


def is_stand_fall_growth_limits(loop_params: dict, low_prices_back_list: list) -> bool:
    min_price_fall_per_step_limit_pct = loop_params['min_price_fall_per_step_limit_pct']

    max_price_growth_per_step_limit_pct = loop_params['max_price_growth_per_step_limit_pct']

    try:
        # min_price_fall_per_step_pct = min(
        #     [100 * (p - f) / (p - low_prices_back_list[-1] + 0.00000000001) for f, p in
        #      zip(low_prices_back_list[1:], low_prices_back_list[:-1])])

        max_price_growth_per_step_pct = max(
            [100 * (f - p) / (p - low_prices_back_list[-1] + 0.00000000001) for f, p in
             zip(low_prices_back_list[1:], low_prices_back_list[:-1])])

    except ZeroDivisionError:
        return False

    # if min_price_fall_per_step_limit_pct <= min_price_fall_per_step_pct:
    #     return False

    if max_price_growth_per_step_pct >= max_price_growth_per_step_limit_pct:
        return False

    return True


def drow_point_info_png(
        index: int,
        loop_params: dict,
        path_for_logs: str,
        df: pd.DataFrame,
        low_prices_back_list: list,
        avg_prices_ahead_list: list,
        current_symbol: str,
        current_ts: str,
        trade_result: str,
        trade_gain_result_value_pct: float
) -> None:

    low_prices_list_to_drow = df.iloc[
                                  index - loop_params['back_check_period_length'] * 4:
                                  index + loop_params['minutes_to_trade_after_buy'] * 2,
                                  3
                              ].to_list()

    avg_prices_list_to_drow = df.iloc[
                                  index - loop_params['back_check_period_length'] * 4:
                                  index + loop_params['minutes_to_trade_after_buy'] * 2,
                                  4
                              ].to_list()

    x_period_start = len(low_prices_back_list) * 3
    x_period_end = x_period_start + loop_params['back_check_period_length'] + 1

    x_axes_list = [x for x in range(0 - x_period_end, len(low_prices_list_to_drow) - x_period_end)]

    plt.style.use('fivethirtyeight')

    plt.plot(x_axes_list, low_prices_list_to_drow, color='gray', linewidth=0.5, label='low price')
    plt.plot(x_axes_list, avg_prices_list_to_drow, color='#32383b', linewidth=0.5, label='avg preice')

    plt.suptitle(f"{current_symbol} started in {current_ts} {trade_result} {abs(trade_gain_result_value_pct)}%")

    plt.axvline(x=0 - len(low_prices_back_list), color='#32383b', linewidth=0.5)
    plt.axvline(x=0, color='#32383b', linewidth=0.5)

    plt.text(
        0 - x_period_end,
        avg_prices_ahead_list[0] * 1.001,
        f'price in {avg_prices_ahead_list[0]}',
        color='#32383b',
        fontsize=5
    )

    plt.axhline(y=avg_prices_ahead_list[0], color='#32383b', linewidth=0.5)

    one_pct_of_the_in_price = avg_prices_ahead_list[0] * 0.01

    for y_border in range(-5, 6):

        color = '#32383b'
        txt = ''

        if y_border == -1:
            txt = 'stop loss '

        elif y_border in (0, 2, 3, 4, -3, -4, -2):
            continue

        elif y_border == 5:
            txt = 'target '

        y_border_value = avg_prices_ahead_list[0] + one_pct_of_the_in_price * y_border

        plt.axhline(y=y_border_value, color=color, linewidth=0.5)

        plt.text(
            0 - x_period_end,
            y_border_value * 1.001,
            f'{txt}{y_border:+d}%',
            color=color,
            fontsize=5
        )

    plt.xlabel('1 minute step', fontsize=7)
    plt.ylabel('USDT price', fontsize=7)

    plt.legend(loc='lower right', frameon=False, fontsize=7)

    plt.axis('off')

    plt.grid(b=None)

    plt.savefig(f"{path_for_logs}/{current_ts}_{current_symbol}.png".replace(':', '-'), dpi=300)

    plt.cla()
