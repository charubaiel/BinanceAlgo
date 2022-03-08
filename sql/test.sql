--
select  *
from(
    select cityHash64(symbol, toString(ts)) as sm_id,
           *
    from binanceFastGrowth29mFeatures
) as moments_features
any left join(
    select
        cityHash64(symbol, toString(ts)) as sm_id,
        100 * (arrayMin( arrayMap(x -> if(x in (nan, inf, null), 0, x), avg_prices_ahead_list) ) / if(avg_prices_ahead_list[1]=0, avg_prices_ahead_list[2], avg_prices_ahead_list[1])) - 100 as min_avg_price_ahead_pct,
        100 * (arrayMax( arrayMap(x -> if(x in (nan, inf, null), 0, x), avg_prices_ahead_list) ) / if(avg_prices_ahead_list[1]=0, avg_prices_ahead_list[2], avg_prices_ahead_list[1])) - 100 as max_avg_price_ahead_pct,
        100 * (arrayMin( arrayMap(x -> if(x in (nan, inf, null), 0, x), low_prices_ahead_list) ) / if(avg_prices_ahead_list[1]=0, avg_prices_ahead_list[2], avg_prices_ahead_list[1])) - 100 as min_low_price_ahead_pct,
        100 * (arrayMax( arrayMap(x -> if(x in (nan, inf, null), 0, x), high_prices_ahead_list)) / if(avg_prices_ahead_list[1]=0, avg_prices_ahead_list[2], avg_prices_ahead_list[1])) - 100 as max_high_price_ahead_pct,
        is_under_the_line,
        is_enough_volume,
        is_stand_fall_growth_limits_original,
        is_stand_fall_growth_limits
    from binanceFastGrowth29m
) features_from_moments using(sm_id)
;


select distinct *
from (
    with
        60 * 2 as min_before_zerro
    select  cityHash64(symbol, toString(ts)) as sm_id,
            symbol,
            ts,
            arrayMap(x -> toInt32(x - min_before_zerro), range(min_before_zerro + 120)) minutes_before_zerro_full_arr,
            arrayMap(x -> if(x in (nan, inf, null), 0, x), arrayConcat(arrayMap(x -> 0, range(toUInt32(min_before_zerro - length(low_prices_back_list)))), low_prices_back_list, low_prices_ahead_list)) as low_prices_full_arr,
            arrayMap(x -> if(x in (nan, inf, null), 0, x), arrayConcat(arrayMap(x -> 0, range(toUInt32(min_before_zerro - length(high_prices_back_list)))), high_prices_back_list, high_prices_ahead_list)) as high_prices_full_arr,
            arrayMap(x -> if(x in (nan, inf, null), 0, x), arrayConcat(arrayMap(x -> 0, range(toUInt32(min_before_zerro - length(open_prices_back_list)))), open_prices_back_list, open_prices_ahead_list)) as open_prices_full_arr,
            arrayMap(x -> if(x in (nan, inf, null), 0, x), arrayConcat(arraySlice(avg_prices_large_back_list, length(avg_prices_large_back_list) - min_before_zerro + 1), avg_prices_ahead_list)) as avg_prices_full_arr,
            arrayConcat(
--                 arrayMap(x -> 0, range(toUInt32(min_before_zerro - length(ttl_tsell_vol_back_list)))),
                arrayMap(
                    x -> if(x in (nan, inf, null), 0, x),
                    arraySlice(ttl_tsell_vol_back_list, length(ttl_tsell_vol_back_list) - min_before_zerro + 1)
                ),
                arrayMap(x -> 0, range(toUInt32(120)))
            ) as ttl_tsell_vol_full_arr,
            arrayConcat(
                arrayMap(
                    x -> if(x in (nan, inf, null), 0, x),
                    arraySlice(ttl_vol_large_back_list, length(ttl_vol_large_back_list) - min_before_zerro + 1)),
                arrayMap(x -> 0, range(toUInt32(120)))
            ) as ttl_vol_large_full_arr
    from    binanceFastGrowth29m
)
array join
    minutes_before_zerro_full_arr,
    low_prices_full_arr,
    high_prices_full_arr,
    open_prices_full_arr,
    avg_prices_full_arr,
    ttl_tsell_vol_full_arr,
    ttl_vol_large_full_arr
