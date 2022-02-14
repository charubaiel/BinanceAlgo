# -*- coding: utf-8 -*-



def is_stand_fall_growth_limits_original(
        loop_params_dict,
        low_prices_back_list: list,
        max_val_growth_coeff: int = 4
) -> bool:

    min_neg_growth_in_tf2_shere = -1 * loop_params_dict['min_price_growth_per_step_limit_pct']
    back_check_period_length = loop_params_dict['back_check_period_length']
    max_value_growth_in_tf2_shere = (100 / back_check_period_length) * (back_check_period_length / max_val_growth_coeff)
    price_usd_open = low_prices_back_list[0]


    try:
        min_neg_growth_in_tf2 = min([100 * (y - x) / (y - price_usd_open + 0.00000000001) for x, y in
                                     zip(low_prices_back_list[1:], low_prices_back_list[:-1])])

        max_value_growth_in_tf2 = max(
            [100 * (x - y) / (low_prices_back_list[-1] - price_usd_open + 0.00000000001) for x, y in
             zip(low_prices_back_list[1:], low_prices_back_list[:-1])])

    except ZeroDivisionError:
        return False

    if min_neg_growth_in_tf2 >= min_neg_growth_in_tf2_shere:
        return False

    elif max_value_growth_in_tf2 >= max_value_growth_in_tf2_shere:
        return False

    return True


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


def underline_check_original(next_prices_usd_open):
    x_list = [i for i in range(int(len(next_prices_usd_open) / 2), len(next_prices_usd_open))][:-1]
    y_list = next_prices_usd_open[int(len(next_prices_usd_open) / 2):][:-1]

    k = (next_prices_usd_open[-1] - next_prices_usd_open[0]) / (len(next_prices_usd_open) - 1)
    b = next_prices_usd_open[0]

    for x, y in zip(x_list, y_list):
        y0 = k * x + b

        if y > y0:
            return 1

    return 0


def is_price_growth_above_the_limit(loop_params: dict, low_prices_back_list: list) -> bool:
    avg_growth_period_limit = loop_params['avg_price_growth_per_step_limit_pct']
    period_length = loop_params['back_check_period_length']

    min_growth_for_period_limit = avg_growth_period_limit * period_length
    growth_for_period = (100 * low_prices_back_list[-1] / low_prices_back_list[0]) - 100

    if growth_for_period < min_growth_for_period_limit:
        return False

    return True


def is_enough_volume(usd_volumes_back_list: list) -> bool:
    if sum(usd_volumes_back_list) / len(usd_volumes_back_list) >= 1000000:
        return True
    return False


def is_stand_fall_growth_limits(loop_params: dict, prices_back_list: list) -> bool:
    min_price_growth_per_step_limit_pct = loop_params['min_price_growth_per_step_limit_pct']

    max_price_growth_per_step_limit_pct = loop_params['max_price_growth_per_step_limit_pct']

    price_first = prices_back_list[0]
    price_last = prices_back_list[-1]

    for price_past, price_current in zip(prices_back_list[1:-1], prices_back_list[2:]):
        price_growth_step_pct = (100 * price_current / price_past) - 100

        price_growth_ttl_pct = (100 * price_last / price_first) - 100

        price_growthg_pkt = 100 * price_growth_step_pct / price_growth_ttl_pct

        if min_price_growth_per_step_limit_pct <= price_growthg_pkt <= max_price_growth_per_step_limit_pct:
            continue

        else:
            return False

    return True
