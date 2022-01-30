# -*- coding: utf-8 -*-
import os
import pandas as pd
import matplotlib.pyplot as plt
from tools import ch_select
from tqdm import tqdm



def underline_check(next_prices_usd_open):
    x_list = [i for i in range(int(len(next_prices_usd_open) / 2), len(next_prices_usd_open))][:-1]
    y_list = next_prices_usd_open[int(len(next_prices_usd_open) / 2):][:-1]
    k = (next_prices_usd_open[-1] - next_prices_usd_open[0]) / (len(next_prices_usd_open) - 1)
    b = next_prices_usd_open[0]

    for x, y in zip(x_list, y_list):
        y0 = k * x + b
        if y > y0:
            return 1
    return 0


def is_growt_before(prev_prices_usd_open, avg_growth_per_tf1):



    price_closed_prev = [prev_prices_usd_open[i: i+5][-1] for i in range(len(prev_prices_usd_open)) if i % 5 == 0]

    if (100 * price_closed_prev[-1] / price_closed_prev[0] - 100) / len(prev_prices_usd_open) >= avg_growth_per_tf1*0.5:
        return 0
    if (100 * price_closed_prev[-1] / price_closed_prev[0] - 100) < 2:
        return 0

    return 1

def is_growt_before5m(prev_prices_usd_open, avg_growth_per_tf1):

    price_closed_prev = [prev_prices_usd_open[i: i+5][-1] for i in range(len(prev_prices_usd_open)) if i % 5 == 0]

    if (100 * price_closed_prev[-1] / price_closed_prev[0] - 100) / len(prev_prices_usd_open) >= avg_growth_per_tf1*0.5:
        return 0
    for f, p in zip(price_closed_prev[1:], price_closed_prev[:-1]):
        if (100 * f / p) - 100 < 0:
            return 0
    return 1


def find_smoothly_growing_time_frames(
    ds = '2021-11-01',
    de = '2022-01-16',
    avg_growth_per_tf1 = 0.5,
    tf2 = 30,
    min_neg_growth_in_tf2_shere = 18,
    max_val_growth_coeff = 4):
    """
    avg_growth_per_tf1 - средний рост за минуту в процентах
    tf2 - размер увеличенного таймфрейма в минутах (в рамках какого интервала искать заданное поведение)
    min_neg_growth_in_tf2_shere минимальное минутное падение в процентах от общего роста за минуту от подения
    max_val_growth_coeff коэфф для расчета максимального роста, чем выше тем меньше разрашенный макс рост за минуту
    """


    raw_data_query = f'''
    select
            symbol,
            timeOpen,
            priceUsd_open,
            priceUsd_low,
            priceUsd_weighted
    from    default.binanceCandle1m
    where   1=1
            and toDate(timeOpen) between '{ds}' and '{de}'
    order   by symbol, timeOpen asc
    -- limit 1000
    '''


    df = pd.read_pickle('tmp.pkl')
    # df = ch_select(raw_data_query, 'df')
    # df.to_pickle('tmp.pkl')
    ttl_rows = df.shape[0]
    print(f'{ttl_rows=}')




    min_ttl_growth_over_tf2 = avg_growth_per_tf1 * tf2
    max_value_growth_in_tf2_shere = (100 / tf2) * (tf2 / max_val_growth_coeff)
    tf3 = tf2*2
    min_ttl_growth_over_tf3 = avg_growth_per_tf1 * tf3 / 10
    max_value_growth_in_tf3_shere = (100 / tf3) * (tf3 / (max_val_growth_coeff / 2))



    pt, ps = None, None
    cnt = 0
    path = f"min2_tf2-{tf2}_avgGr-{avg_growth_per_tf1}_minNegGr-{min_neg_growth_in_tf2_shere}_coeff={max_val_growth_coeff}/"
    vins, loss, other = 0, 0, []


    try:
        os.mkdir(path)
    except OSError:
        print("Creation of the directory %s failed" % path)
    else:
        print("Successfully created the directory %s " % path)


    for index, row in tqdm(df.iterrows(),total=df.shape[0]):
        # if index%10000 == 0:
        #     print(f'{100*round(index/ttl_rows, 4)}') # replace with tqdm
        t, s = row['timeOpen'][:10], row['symbol']
        price_usd_open = row['priceUsd_low']
        next_prices_usd_open = df.iloc[index + 1:index + tf2 + 1, 3].to_list()
        # prev_prices_usd_open row[= df.iloc[index - tf3: index, 2].to_list()
        # print(tf3, '------------')

        # if not len(prev_prices_usd_open) == tf3:
        #     continue
        # price_usd_open_tf3 = prev_prices_usd_open[0]
        # prev_prices_usd_open = prev_prices_usd_open[1:]

        if len(set(df.iloc[index + 1:index + tf2 + 1, 0].to_list())) > 1:
            continue
        if not len(next_prices_usd_open) == tf2:
            continue
        if underline_check(next_prices_usd_open):
            continue
        # if not is_growt_before(prev_prices_usd_open, avg_growth_per_tf1):
        #     continue


        ttl_growth_over_tf2 = (100 * next_prices_usd_open[-1] / price_usd_open) - 100
        # ttl_growth_over_tf3 = (100 * prev_prices_usd_open[-1] / prev_prices_usd_open[0]) - 100



        try:
            min_neg_growth_in_tf2 = min([100 * (y - x) / (y - price_usd_open + 0.00000000001) for x, y in
                                         zip(next_prices_usd_open[1:], next_prices_usd_open[:-1])])
            max_value_growth_in_tf2 = max([100 * (x - y) / (next_prices_usd_open[-1] - price_usd_open + 0.00000000001) for x, y in
                 zip(next_prices_usd_open, [price_usd_open, ] + next_prices_usd_open[:-1])])
        except ZeroDivisionError:
            continue
        if ttl_growth_over_tf2 < min_ttl_growth_over_tf2:
            continue
        elif min_neg_growth_in_tf2 >= min_neg_growth_in_tf2_shere:
            continue
        elif max_value_growth_in_tf2 >= max_value_growth_in_tf2_shere:
            continue




        # try:
        #     min_neg_growth_in_tf3 = min([100 * (y - x) / (y - price_usd_open_tf3 + 0.00000000001) for x, y in
        #                                  zip(prev_prices_usd_open[1:], prev_prices_usd_open[:-1])])
        #     max_value_growth_in_tf3 = max([100 * (x - y) / (prev_prices_usd_open[-1] - price_usd_open_tf3 + 0.00000000001) for x, y in
        #          zip(prev_prices_usd_open, [price_usd_open_tf3, ] + prev_prices_usd_open[:-1])])
        # except ZeroDivisionError:
        #     continue
        # if ttl_growth_over_tf3 < min_ttl_growth_over_tf3:
        #     continue
        # elif min_neg_growth_in_tf3 >= min_neg_growth_in_tf2_shere:
        #     continue
        # elif max_value_growth_in_tf3 >= max_value_growth_in_tf3_shere:
        #     continue



        if pt != t or ps != s:
            next_prices_usd_open_for_predict = df.iloc[index + tf2: index + 2*2*tf2 + 1, 3].to_list()

            open_prices = df.iloc[index + tf2: index + 2 * 2 * tf2 + 1, 4].to_list()
            price = row['priceUsd_open']

            stop_loss3 = next_prices_usd_open_for_predict[0] - next_prices_usd_open_for_predict[0]*3/100
            bid3 = next_prices_usd_open_for_predict[0] + next_prices_usd_open_for_predict[0]*3/100
            stop_loss = next_prices_usd_open_for_predict[0] - next_prices_usd_open_for_predict[0]*1.1/100
            bid = next_prices_usd_open_for_predict[0] + next_prices_usd_open_for_predict[0]*1.1/100
            stop_loss2 = next_prices_usd_open_for_predict[0] - next_prices_usd_open_for_predict[0]*2/100
            bid2 = next_prices_usd_open_for_predict[0] + next_prices_usd_open_for_predict[0]*2/100
            stop_loss5 = next_prices_usd_open_for_predict[0] - next_prices_usd_open_for_predict[0]*5/100
            bid5 = next_prices_usd_open_for_predict[0] + next_prices_usd_open_for_predict[0]*5/100
            bid5 = price + next_prices_usd_open_for_predict[0]*5/100


            for price, sell_price in zip(next_prices_usd_open_for_predict, open_prices):
                if price <= stop_loss:
                    loss += 1
                    loop_result = 'LOSS'
                    break
                elif price >= bid5:
                    vins += 100*(sell_price / price) - 100
                    loop_result = 'WIN'
                    break
            else:
                other.append((100*next_prices_usd_open_for_predict[-1]/next_prices_usd_open_for_predict[0]) - 100)
                loop_result = f'OTHER: {round((100*next_prices_usd_open_for_predict[-1]/next_prices_usd_open_for_predict[0]) - 100, 2)}'

            cnt += 1

            mult = 4

            plt.plot(df.iloc[index + 1 - tf2*mult: index + tf2 + tf2*mult + 1, 3].to_list(), color = 'black', linewidth = 1)
            plt.suptitle(f"{cnt}. {row['symbol']} {t} => {loop_result} {100*round(index/ttl_rows, 4)}")
            plt.axvline(x = tf2*mult, color = 'black', linewidth = 1)
            plt.axvline(x = tf2*mult + tf2 - 1, color = 'black', linewidth = 1)
            plt.axhline(y = bid, color = 'green', linewidth = 1)
            plt.axhline(y = stop_loss, color = 'red', linewidth = 1)
            plt.axhline(y = bid3, color = 'green', linewidth = 1)
            plt.axhline(y = stop_loss3, color = 'red', linewidth = 1)
            plt.axhline(y = bid2, color = 'green', linewidth = 1)
            plt.axhline(y = stop_loss2, color = 'red', linewidth = 1)
            plt.axhline(y = bid5, color = 'green', linewidth = 1)
            plt.axhline(y = stop_loss5, color = 'red', linewidth = 1)
            plt.axhline(y = next_prices_usd_open_for_predict[0], color = 'gray', linewidth = 1)
            plt.text(tf2*mult + tf2 - 1, next_prices_usd_open[-1], f'{next_prices_usd_open[-1]}', color = 'black', horizontalalignment='right')
            plt.text(tf2*mult, next_prices_usd_open[0], f'{next_prices_usd_open[0]}', color = 'black', horizontalalignment='right')





            plt.savefig(f"{path}/{t}_{row['symbol']}.png")
            # plt.show()
            plt.cla()
            print(cnt, t, row['symbol'], f'{vins=}/{loss}/other={len(other)}    {loop_result} ({100*round(index/ttl_rows, 4)})')
        pt, ps = t, s
    ttl = sum(other) + (vins-loss) * 3
    return ttl, f'{vins=}/{loss}/other={len(other)} <=> {ds=}, {de=}, {avg_growth_per_tf1=}, {tf2=}, {min_neg_growth_in_tf2_shere=}, {max_val_growth_coeff=}'




print(
find_smoothly_growing_time_frames(
    ds = '2021-11-01',
    de = '2022-01-17',
    avg_growth_per_tf1 = 0.5,
    tf2 = 29,
    min_neg_growth_in_tf2_shere = 18,
    max_val_growth_coeff = 4)

)



#
# print(
# find_smoothly_growing_time_frames(
#     ds = '2021-11-01',
#     de = '2022-11-01',
#     avg_growth_per_tf1 = 0.5,
#     tf2 = 29,
#     min_neg_growth_in_tf2_shere = 18,
#     max_val_growth_coeff = 4)
#
# )

