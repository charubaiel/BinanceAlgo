select symbol,
       ts,
       low_prices_back_list,
       low_prices_ahead_list,
       high_prices_back_list,
       high_prices_ahead_list,
       open_prices_back_list,
       open_prices_ahead_list,
       avg_prices_large_back_list,
       avg_prices_ahead_list,
       ttl_tsell_vol_back_list,
       ttl_vol_large_back_list,
       is_under_the_line,
       is_enough_volume,
       is_stand_fall_growth_limits_original,
       is_stand_fall_growth_limits
from default.binanceFastGrowth29m