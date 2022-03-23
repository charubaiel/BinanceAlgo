# -*- coding: utf-8 -*-
import math
from datetime import timedelta
import yaml

from tools.data_loop_handlers import *
from datetime import datetime
from tools.data_loop_filters import *
from tools.color_loger import *
from tools.clickhouse_handlers import *

sql_name = 'binance_fast_growth_29m'

with open(f'sql/{sql_name}.sql', 'r') as f:
    query = f.read()

# df = ch_select(query)
# df.to_pickle(f'{sql_name}.pkl')
df = pd.read_pickle(f'{sql_name}.pkl')

for index, row in df.iterrows():
    ts = row['ts']
    symbol = row['symbol']
    key_row = [symbol, ts]

    avg_prices_large_back_list = np.fromstring(row['avg_prices_large_back_list'][1:-1], dtype=float, sep=', ')
    avg_prices_change_large_back_list = np.array(
        [0 if p == 0 else f * 100 / p - 100 for f, p in zip(avg_prices_large_back_list[1:], avg_prices_large_back_list[:-1])]
    )

    vol_large_back_list = np.fromstring(row['ttl_vol_large_back_list'][1:-1], dtype=float, sep=', ')
    vol_change_large_back_list = np.array(
        [0 if p == 0 else f * 100 / p - 100 for f, p in zip(vol_large_back_list[1:], vol_large_back_list[:-1]) if p != 0]
    )



    avg_prices_change_large_back_list / (vol_change_large_back_list + avg_prices_change_large_back_list)








    avg_prices_back_list = avg_prices_large_back_list[-29:]
    avg_prices_10m_before_back_list = avg_prices_large_back_list[-29 - 10:-29]
    avg_prices_30m_before_back_list = avg_prices_large_back_list[-29 - 30:-29]
    avg_prices_1h_before_back_list = avg_prices_large_back_list[-29 - 60:-29]
    avg_prices_3h_before_back_list = avg_prices_large_back_list[-29 - 60 * 3:-29]
    avg_prices_5h_before_back_list = avg_prices_large_back_list[-29 - 60 * 5:-29]

    vol_back_list = vol_large_back_list[-29:]
    vol_10m_before_back_list = vol_large_back_list[-29 - 10:-29]
    vol_30m_before_back_list = vol_large_back_list[-29 - 30:-29]
    vol_1h_before_back_list = vol_large_back_list[-29 - 60:-29]
    vol_3h_before_back_list = vol_large_back_list[-29 - 60 * 3:-29]
    vol_5h_before_back_list = vol_large_back_list[-29 - 60 * 5:-29]
    vol_5h_before_zerro = vol_large_back_list[-29 - 60 * 5:]
    vol_median_back_list = np.median(vol_back_list)

    avg_prices_change_back_list = avg_prices_change_large_back_list[-29:]
    avg_prices_change_10m_before_back_list = avg_prices_change_large_back_list[-29 - 10:-29]
    avg_prices_change_30m_before_back_list = avg_prices_change_large_back_list[-29 - 30:-29]
    avg_prices_change_1h_before_back_list = avg_prices_change_large_back_list[-29 - 60:-29]
    avg_prices_change_3h_before_back_list = avg_prices_change_large_back_list[-29 - 60 * 3:-29]
    avg_prices_change_5h_before_back_list = avg_prices_change_large_back_list[-29 - 60 * 5:-29]

    # Сумма процентов падений и роста цены
    price_pct_change_growth_in_back_list = int(sum([ppg for ppg in avg_prices_change_back_list if ppg > 0]) * 100)/100
    price_pct_change_growth_10m_before_back_list = int(sum([ppg for ppg in avg_prices_change_10m_before_back_list if ppg > 0]) * 100)/100
    price_pct_change_growth_30m_before_back_list = int(sum([ppg for ppg in avg_prices_change_30m_before_back_list if ppg > 0]) * 100)/100
    price_pct_change_growth_1h_before_back_list = int(sum([ppg for ppg in avg_prices_change_1h_before_back_list if ppg > 0]) * 100)/100
    price_pct_change_growth_3h_before_back_list = int(sum([ppg for ppg in avg_prices_change_3h_before_back_list if ppg > 0]) * 100)/100
    price_pct_change_growth_5h_before_back_list = int(sum([ppg for ppg in avg_prices_change_5h_before_back_list if ppg > 0]) * 100)/100

    price_pct_change_fall_in_back_list = int(sum([ppg for ppg in avg_prices_change_back_list if ppg < 0]) * 100)/100
    price_pct_change_fall_10m_before_back_list = int(sum([ppg for ppg in avg_prices_change_10m_before_back_list if ppg < 0]) * 100)/100
    price_pct_change_fall_30m_before_back_list = int(sum([ppg for ppg in avg_prices_change_30m_before_back_list if ppg < 0]) * 100)/100
    price_pct_change_fall_1h_before_back_list = int(sum([ppg for ppg in avg_prices_change_1h_before_back_list if ppg < 0]) * 100)/100
    price_pct_change_fall_3h_before_back_list = int(sum([ppg for ppg in avg_prices_change_3h_before_back_list if ppg < 0]) * 100)/100
    price_pct_change_fall_5h_before_back_list = int(sum([ppg for ppg in avg_prices_change_5h_before_back_list if ppg < 0]) * 100)/100

    # Стандартное отклонение процентов изменения
    price_pct_change_std_in_back_list = np.std(avg_prices_change_back_list[np.logical_not(np.isnan(avg_prices_change_back_list))])
    price_pct_change_std_10m_before_back_list = np.std(avg_prices_change_10m_before_back_list[np.logical_not(np.isnan(avg_prices_change_10m_before_back_list))])
    price_pct_change_std_30m_before_back_list = np.std(avg_prices_change_30m_before_back_list[np.logical_not(np.isnan(avg_prices_change_30m_before_back_list))])
    price_pct_change_std_1h_before_back_list = np.std(avg_prices_change_1h_before_back_list[np.logical_not(np.isnan(avg_prices_change_1h_before_back_list))])
    price_pct_change_std_3h_before_back_list = np.std(avg_prices_change_3h_before_back_list[np.logical_not(np.isnan(avg_prices_change_3h_before_back_list))])
    price_pct_change_std_5h_before_back_list = np.std(avg_prices_change_5h_before_back_list[np.logical_not(np.isnan(avg_prices_change_5h_before_back_list))])

    # Отношение медианной цены за 10 мин, 0.5, 1, 3, 5 часов к последней цене в периоде
    median_price_in_back_list_to_last_price_period = 0 if avg_prices_large_back_list[-1] == 0 else np.median(avg_prices_back_list[np.logical_not(np.isnan(avg_prices_back_list))])/avg_prices_large_back_list[-1]
    median_price_10m_before_back_list_to_last_price_period = 0 if avg_prices_large_back_list[-1] == 0 else np.median(avg_prices_10m_before_back_list[np.logical_not(np.isnan(avg_prices_10m_before_back_list))])/avg_prices_large_back_list[-1]
    median_price_30m_before_back_list_to_last_price_period = 0 if avg_prices_large_back_list[-1] == 0 else np.median(avg_prices_30m_before_back_list[np.logical_not(np.isnan(avg_prices_30m_before_back_list))])/avg_prices_large_back_list[-1]
    median_price_1h_before_back_list_to_last_price_period = 0 if avg_prices_large_back_list[-1] == 0 else np.median(avg_prices_1h_before_back_list[np.logical_not(np.isnan(avg_prices_1h_before_back_list))])/avg_prices_large_back_list[-1]
    median_price_3h_before_back_list_to_last_price_period = 0 if avg_prices_large_back_list[-1] == 0 else np.median(avg_prices_3h_before_back_list[np.logical_not(np.isnan(avg_prices_3h_before_back_list))])/avg_prices_large_back_list[-1]
    median_price_5h_before_back_list_to_last_price_period = 0 if avg_prices_large_back_list[-1] == 0 else np.median(avg_prices_5h_before_back_list[np.logical_not(np.isnan(avg_prices_5h_before_back_list))])/avg_prices_large_back_list[-1]

    # Отношение медианного объема за 10 мин, 0.5, 1, 3, 5 часов к медианному объему периода
    median_vol_10m_before_back_list_to_last_price_period = np.median(vol_10m_before_back_list[np.logical_not(np.isnan(vol_10m_before_back_list))])/vol_median_back_list
    median_vol_30m_before_back_list_to_last_price_period = np.median(vol_30m_before_back_list[np.logical_not(np.isnan(vol_30m_before_back_list))])/vol_median_back_list
    median_vol_1h_before_back_list_to_last_price_period = np.median(vol_1h_before_back_list[np.logical_not(np.isnan(vol_1h_before_back_list))])/vol_median_back_list
    median_vol_3h_before_back_list_to_last_price_period = np.median(vol_3h_before_back_list[np.logical_not(np.isnan(vol_3h_before_back_list))])/vol_median_back_list
    median_vol_5h_before_back_list_to_last_price_period = np.median(vol_5h_before_back_list[np.logical_not(np.isnan(vol_5h_before_back_list))])/vol_median_back_list

    # еще что-то
    vol_min = np.min(vol_5h_before_zerro[np.logical_not(np.isnan(vol_5h_before_zerro))])
    vol_max = np.max(vol_5h_before_zerro[np.logical_not(np.isnan(vol_5h_before_zerro))])
    vol_min_to_max = (vol_min / vol_max) * 100




    # отношение ростов объема (ts, tb). Как сильно влияет рост объема на рост цены

    # средний объем



    avg_prices_back_list_not_nan = avg_prices_back_list[np.logical_not(np.isnan(avg_prices_back_list))]

    period_gain_pct = 0 if avg_prices_back_list_not_nan[0] == 0 else (avg_prices_back_list_not_nan[-1] * 100 / avg_prices_back_list_not_nan[0]) - 100

    zerro_hour = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S').hour


    # results
    stop_losses = np.array([0.5, 1, 2, 4, 8, 16])

    stop_losses_decart = np.transpose([np.tile(stop_losses, len(stop_losses)), np.repeat(stop_losses, len(stop_losses))])
    res = {}
    low_prices_ahead_list = np.fromstring(row['low_prices_ahead_list'][1:-1], dtype=float, sep=', ')
    avg_prices_ahead_list = np.fromstring(row['avg_prices_ahead_list'][1:-1], dtype=float, sep=', ')
    price_in = avg_prices_ahead_list[0]
    if price_in == 0:
        continue

    for sl in stop_losses_decart:

        price_win = (100 + sl[0]) * price_in / 100
        price_lose = (100 - sl[1]) * price_in / 100

        gain_pct = 0[None,] + [int(x) for x in np.linspace(start = 1, stop = 30, num = 15)],
        is_win = 0
        is_timeout = 0

        avg_prices_ahead_list_to_check = avg_prices_ahead_list[1:]

        avg_prices_ahead_list_to_check_cur = avg_prices_ahead_list[:-1]
        avg_prices_ahead_list_to_check_fut = pd.Series(avg_prices_ahead_list[1:]).fillna(method="bfill").fillna(method="ffill").values


        for curent_price, future_price in zip(avg_prices_ahead_list_to_check_cur, avg_prices_ahead_list_to_check_fut):

            if math.isnan(curent_price):
                continue


            if curent_price >= price_win:
                gain_pct = (future_price * 100 / price_in) - 100
                is_win = 1
                break

            if curent_price < price_lose:
                gain_pct = (future_price * 100 / price_in) - 100
                is_win = 0
                break

            gain_pct = (future_price * 100 / price_in) - 100
            is_timeout = 1

        #
        # for low, avg in zip(low_prices_ahead_list, avg_prices_ahead_list):
        #
        #     if math.isnan(low) or math.isnan(avg):
        #         continue
        #     #
        #     # if low <= price_lose:
        #     #     gain_pct = sl[1] * -1
        #     #     break
        #
        #     if avg >= price_win:
        #         gain_pct = sl[0]
        #         is_win = 1
        #         break
        #
        #     gain_pct = (avg * 100 / price_in) - 100
        #     is_timeout = 1



        sl0 = str(sl[0]).replace(".0", "").replace("0.5", "05")
        sl1 = str(sl[1]).replace(".0", "").replace("0.5", "05")
        res[f'win_pct_gain{sl0}_los{sl1}'] = gain_pct
        res[f'win_cnt_gain{sl0}_los{sl1}'] = is_win
    res_row = [int(1000 * res[r]) / 1000 for r in res]

    f_row = [
             price_pct_change_growth_in_back_list,
             price_pct_change_growth_10m_before_back_list,
             price_pct_change_growth_30m_before_back_list,
             price_pct_change_growth_1h_before_back_list,
             price_pct_change_growth_3h_before_back_list,
             price_pct_change_growth_5h_before_back_list,
             price_pct_change_fall_in_back_list,
             price_pct_change_fall_10m_before_back_list,
             price_pct_change_fall_30m_before_back_list,
             price_pct_change_fall_1h_before_back_list,
             price_pct_change_fall_3h_before_back_list,
             price_pct_change_fall_5h_before_back_list,
             int(1000 * price_pct_change_std_in_back_list)/1000,
             int(1000 * price_pct_change_std_10m_before_back_list)/1000,
             int(1000 * price_pct_change_std_30m_before_back_list)/1000,
             int(1000 * price_pct_change_std_1h_before_back_list)/1000,
             int(1000 * price_pct_change_std_3h_before_back_list)/1000,
             int(1000 * price_pct_change_std_5h_before_back_list)/1000,
             int(1000 * median_price_in_back_list_to_last_price_period)/1000,
             int(1000 * median_price_10m_before_back_list_to_last_price_period)/1000,
             int(1000 * median_price_1h_before_back_list_to_last_price_period)/1000,
             int(1000 * median_price_30m_before_back_list_to_last_price_period)/1000,
             int(1000 * median_price_3h_before_back_list_to_last_price_period)/1000,
             int(1000 * median_price_5h_before_back_list_to_last_price_period)/1000,
             int(1000 * median_vol_10m_before_back_list_to_last_price_period)/1000,
             int(1000 * median_vol_30m_before_back_list_to_last_price_period)/1000,
             int(1000 * median_vol_1h_before_back_list_to_last_price_period)/1000,
             int(1000 * median_vol_3h_before_back_list_to_last_price_period)/1000,
             int(1000 * median_vol_5h_before_back_list_to_last_price_period)/1000,
             int(1000 * vol_min) / 1000,
             int(1000 * vol_max) / 1000,
             int(1000 * vol_min_to_max) / 1000,
             int(1000 * period_gain_pct) / 1000,
             zerro_hour]

    row_to_insert = [key_row + f_row + res_row, ]
    ch_insert(row_to_insert, 'binanceFastGrowth29mFeatures3')


