# -*- coding: utf-8 -*-
import os
from datetime import datetime
from random import choices
import json
from string import ascii_lowercase
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tools.clickhouse_handlers import ch_select
from tools.color_loger import *


def get_loop_df(
        sql_file_name: str,
        date_start: str,
        date_end: str,
        symbol: str = '',
        use_cache: bool = False
) -> pd.DataFrame:

    with open(f'sql/{sql_file_name}.sql', 'r') as f:
        query = f.read()

    query = query.format(date_start=date_start, date_end=date_end, symbol=symbol)

    if use_cache:
        df = pd.read_pickle('tmp_get_loop_df.pkl')
    else:
        df = ch_select(query)
        df.to_pickle('tmp_get_loop_df.pkl')

    ttl_rows_in_df = df.shape[0]

    log.info(f'DataFrame for the loop received. {ttl_rows_in_df=}')

    return df


def create_folder_for_logs(loop_params: dict, log, formatter) -> str:
    run_name = ''.join(choices(ascii_lowercase, k=3))

    run_time = datetime.now().strftime('%Y%m%dT%H%M%S')

    run_folder_name = f'{run_time}_{run_name}'

    path_for_logs = f'data_test_loop/{run_folder_name}'

    try:
        os.mkdir(path_for_logs)

        add_file_handler(path_for_logs + '/log.txt', log, formatter)

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
                           ].values

    low_prices_ahead_list = df.iloc[
                                index:
                                index + loop_params['minutes_to_trade_after_buy'],
                                3
                            ].values

    avg_prices_ahead_list = df.iloc[
                                index:
                                index + loop_params['minutes_to_trade_after_buy'],
                                4
                            ].values

    usd_volumes_back_list = df.iloc[
                                index - loop_params['back_check_period_length']:
                                index,
                                5
                            ].values

    # название символа в списке на всем промежутке
    # просомтра от прошлого до будущего, должен быть одинаковый,
    # если выборка не захватывает предыдущей символ
    symb_list_info_period = df.iloc[
                                index - loop_params['back_check_period_length']:
                                index + loop_params['minutes_to_trade_after_buy'],
                                0
                            ].values

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


def get_current_large_point_info(loop_params: dict, index: int, df: pd.DataFrame) -> dict:
    """
    :param loop_params: Dictionary with scan launch parameters
    :param index: The number of the current line in the raw OLH data
    :param df: DataFrame with timestamp sorted OLH data
    :return: Current point info
    """

    large_back_check_period_length = 10_000

    # минимальные цены в usdt
    low_prices_back_list = df.iloc[
                               index - loop_params['back_check_period_length']:
                               index,
                               3
                           ].values

    low_prices_ahead_list = df.iloc[
                               index:
                               index + loop_params['minutes_to_trade_after_buy'],
                               3
                            ].values

    # максимвльные цены в usdt
    high_prices_back_list = df.iloc[
                               index - loop_params['back_check_period_length']:
                               index,
                               5
                           ].values

    high_prices_ahead_list = df.iloc[
                               index:
                               index + loop_params['minutes_to_trade_after_buy'],
                               5
                            ].values

    # цены открытия в usdt
    open_prices_back_list = df.iloc[
                               index - loop_params['back_check_period_length']:
                               index,
                               2
                           ].values

    open_prices_ahead_list = df.iloc[
                               index:
                               index + loop_params['minutes_to_trade_after_buy'],
                               2
                            ].values

    # взвешенные на объем цены в usdt
    avg_prices_large_back_list = df.iloc[
                               index - large_back_check_period_length:
                               index,
                               4
                            ].values

    avg_prices_ahead_list = df.iloc[
                               index:
                               index + loop_params['minutes_to_trade_after_buy'],
                               4
                            ].values

    # объем в usdt, когда инициатор продает монеты
    ttl_tsell_vol_back_list = df.iloc[
                               index - loop_params['back_check_period_length'] * 10:
                               index,
                               7
                            ].values

    # объем в usdt
    ttl_vol_large_back_list = df.iloc[
                               index - large_back_check_period_length:
                               index,
                               6
                            ].values

    # название символа в списке на всем промежутке
    # просомтра от прошлого до будущего, должен быть одинаковый,
    # если выборка не захватывает предыдущей символ
    symb_list_info_period = df.iloc[
                                index - large_back_check_period_length:
                                index + loop_params['minutes_to_trade_after_buy'],
                                0
                            ].values

    # пропускаем строчку если в рассматриваемый период попали 2 монеты
    # т.е. не рассматриваем границы между разными монетами
    if len(set(symb_list_info_period)) > 1:
        return ()

    # пропускам строчку, если невозможно забрать полную
    # историю цен, т.е. пропускаем первые строчки
    is_less_then_large_back_check_period_length = len(avg_prices_large_back_list) < large_back_check_period_length
    is_ahaed_list_less_then_trade_deadline = len(low_prices_ahead_list) < loop_params['minutes_to_trade_after_buy']

    if is_less_then_large_back_check_period_length or is_ahaed_list_less_then_trade_deadline:
        return ()

    # пропускаем строчку, если невозможно забрать полный список
    # цен после рассматриваемого момента для проверки возможности продать
    if len(low_prices_ahead_list) != loop_params['minutes_to_trade_after_buy']:
        return ()

    return {
        'low_prices_back_list': low_prices_back_list,
        'low_prices_ahead_list': low_prices_ahead_list,
        'high_prices_back_list': high_prices_back_list,
        'high_prices_ahead_list': high_prices_ahead_list,
        'open_prices_back_list': open_prices_back_list,
        'open_prices_ahead_list': open_prices_ahead_list,
        'avg_prices_large_back_list': avg_prices_large_back_list,
        'avg_prices_ahead_list': avg_prices_ahead_list,
        'ttl_tsell_vol_back_list': ttl_tsell_vol_back_list,
        'ttl_vol_large_back_list': ttl_vol_large_back_list
    }


def get_trade_result(
    loop_params: dict,
    low_prices_ahead_list: float,
    avg_prices_ahead_list: float,
    usd_balance: float,
    winning_trades_cnt: int,
    losing_trades_cnt: int,
    outoftime_trades_cnt: int,
    symbol: str,
    dt: str
) -> tuple:

    usd_price_in = avg_prices_ahead_list[0]

    coins = usd_balance / usd_price_in

    traid_stop_loss = usd_price_in - usd_price_in * loop_params['stop_loss_pct_of_price_in'] / 100
    traid_bid = usd_price_in + usd_price_in * loop_params['bid_pct_of_price_in'] / 100

    trade_result = None

    for low_price, avg_price in zip(low_prices_ahead_list, avg_prices_ahead_list):

        if low_price <= traid_stop_loss:

            usd_out_price = traid_stop_loss
            losing_trades_cnt += 1
            trade_result = 'loss'

            break

        elif avg_price >= traid_bid:

            usd_out_price = traid_bid
            winning_trades_cnt += 1
            trade_result = 'win'

            break

    if not trade_result:
        usd_out_price = avg_prices_ahead_list[-1]
        outoftime_trades_cnt += 1
        trade_result = 'outoftime'

    usd_balance_upd = round(coins * usd_out_price)

    usd_trade_profit = round(usd_balance_upd - usd_balance, 2)

    trade_profit_pct = round(usd_balance_upd * 100 / usd_balance - 100, 1)

    trades_cnt = winning_trades_cnt + losing_trades_cnt + outoftime_trades_cnt

    win_trades_pct = round(100 * winning_trades_cnt / trades_cnt, 1)

    log.info(
        f'{trades_cnt}.\t{trade_result=}\t{usd_balance_upd=}\t{win_trades_pct=}\t{dt}\t{symbol}\t|\t'
        f'{usd_trade_profit=}\t{trade_profit_pct=}\t{outoftime_trades_cnt=}'
    )

    return usd_balance_upd, trade_result, winning_trades_cnt, losing_trades_cnt, outoftime_trades_cnt, trade_profit_pct


def drow_point_info_png(
        loop_params: dict,
        path_for_logs: str,
        point_info: dict,
        current_symbol: str,
        current_ts: str,
        xmin: int = -300
) -> None:

    avg_prices_list_to_drow = np.concatenate((point_info['avg_prices_large_back_list'], point_info['avg_prices_ahead_list']), axis=None)
    low_prices_list_to_drow = np.concatenate((point_info['low_prices_back_list'], point_info['low_prices_ahead_list']), axis=None)
    high_prices_list_to_drow = np.concatenate((point_info['high_prices_back_list'], point_info['high_prices_ahead_list']), axis=None)
    open_prices_list_to_drow = np.concatenate((point_info['open_prices_back_list'], point_info['open_prices_ahead_list']), axis=None)
    ttl_tsell_vol_to_drow = point_info['ttl_tsell_vol_back_list']
    ttl_vol_to_drow = point_info['ttl_vol_large_back_list']


    x_axes_large_left_right_direction = [x + 1 for x in range(0 - len(point_info['avg_prices_large_back_list']), len(point_info['avg_prices_ahead_list']))]
    x_axes_left_right_direction = [x + 1 for x in range(0 - loop_params['back_check_period_length'], len(point_info['low_prices_ahead_list']))]
    x_axes_left_direction = [x + 1 for x in range(0 - loop_params['back_check_period_length'] * 10, 0)]
    x_axes_large_left_direction = [x + 1 for x in range(0 - len(point_info['ttl_vol_large_back_list']), 0)]

    plt.style.use('fivethirtyeight')

    plt.subplots(2, 1)

    plt.figure(figsize=(16, 9))
    y2 = plt.gca()
    y1 = y2.twinx()

    y1.set_ylabel('USDT price', fontsize=7, backgroundcolor='#fafad2')
    y2.set_ylabel('USDT vol', fontsize=7, backgroundcolor='#fafad2')
    y1.set_xlabel('minutes, 0 - end of training period', fontsize=7, backgroundcolor='#fafad2')

    plt.autolayout = True

    plt.subplots_adjust(left=0.1, right=0.9)

    y1.tick_params(axis='both', which='major', labelsize=7)
    y2.tick_params(axis='both', which='major', labelsize=7)

    y1.grid(b=None)
    y2.grid(b=None)

    plt.box(False)

    plt.setp(y2.spines.values(), color='#fafad2')
    plt.setp(y1.spines.values(), color='#fafad2')


    y1.set_facecolor('#fafad2')
    y2.set_facecolor('#fafad2')

    y1.axhline(y=point_info['avg_prices_ahead_list'][0], color='#32383b', linewidth=0.3)

    y1.axvline(x=0 - len(point_info['low_prices_back_list']), color='#32383b', linewidth=0.3)
    y1.axvline(x=0, color='#32383b', linewidth=0.3)

    one_pct_of_the_in_price = point_info['avg_prices_ahead_list'][0] * 0.01
    for y_border in [16, 8, 4, 2, 1, -1, -2, -4, -8, -16]:

        color = '#32383b'
        txt = 'in '

        y_border_value = point_info['avg_prices_ahead_list'][0] + one_pct_of_the_in_price * y_border
        y1.axhline(y=y_border_value, color=color, linewidth=0.03)

        y1.text(
            loop_params['minutes_to_trade_after_buy'] - 15,
            y_border_value,
            f'{txt}{y_border:+d}%',
            color=color,
            fontsize=5,
            backgroundcolor='#fafad2'
        )

    y2.bar(x_axes_large_left_direction, ttl_vol_to_drow, color='#48dbfb', label='vol', width=1)
    y2.bar(x_axes_left_direction, ttl_tsell_vol_to_drow, color='#54a0ff', label='taker sell vol', width=1)

    y1.plot(x_axes_left_right_direction, low_prices_list_to_drow, color='#d63031', linewidth=0.5, label='low price')
    y1.plot(x_axes_left_right_direction, high_prices_list_to_drow, color='#00b894', linewidth=0.5, label='high price')
    y1.plot(x_axes_left_right_direction, open_prices_list_to_drow, color='#fdcb6e', linewidth=0.5, label='open price')
    y1.plot(x_axes_large_left_right_direction, avg_prices_list_to_drow, color='#636e72', linewidth=0.5, label='avg price')

    plt.suptitle(f"{current_symbol} started in {current_ts}", backgroundcolor='#fafad2')

    plt.text(
        loop_params['minutes_to_trade_after_buy'] - 15,
        point_info['avg_prices_ahead_list'][0],
        f"in= {point_info['avg_prices_ahead_list'][0]}",
        color='#32383b',
        fontsize=5,
        backgroundcolor='#fafad2'
    )

    y1.legend(loc='lower right', frameon=False, fontsize=7)
    y2.legend(loc='upper left', frameon=False, fontsize=7)

    plt.xlim(xmin=xmin)
    plt.xlim(xmax=loop_params['minutes_to_trade_after_buy'])

    y1min = np.concatenate(
        (avg_prices_list_to_drow[xmin-loop_params['minutes_to_trade_after_buy']:], low_prices_list_to_drow),
        axis=None
    ).min()

    y1max = np.concatenate(
        (avg_prices_list_to_drow[xmin-loop_params['minutes_to_trade_after_buy']:], high_prices_list_to_drow),
        axis=None
    ).max()

    y2max = ttl_vol_to_drow[xmin:].max()

    y1.set_ylim([y1min - y1min * 0.001, y1max * 1.001])
    y2.set_ylim([0, y2max * 1.001])

    plt.savefig(f"{path_for_logs}/{current_ts}_{current_symbol}.png".replace(':', '-'), dpi=100,
                facecolor=y2.get_facecolor(), edgecolor='none')

    plt.cla()
