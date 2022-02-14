select
     symbol,
     timeOpen,
     priceUsd_open,
     priceUsd_low,
     round(priceUsd_weighted, 4) as priceUsd_weighted,
     volumeUsd,

     tradesCnt,
     volumeCoin_whenTakerBuyCoin,
     volumeUsd_whenTakerSellCoin,
     priceUsd_close,
     priceUsd_high

from default.binanceCandle1m
where    1=1
     and toDate(timeOpen) between '{date_start}' and '{date_end}'
     and symbol ilike '%{symbol}%'
order by
     symbol, timeOpen asc