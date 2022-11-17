# stage indicators

## Overview

This stage calcualates typical stock market indicators based on the raw OHLC
values. Each indicator exposes its data again on individual streams, generally 
available on daily and weekly time ranges.

## Implemented Indicators

All indicators are available on daily and weekly bases

- EMA 13
- EMA 26
- MACD 12,26,9
- SSTOC 5,5,3

## Ideas for Indicators

- ATR
- Local Highs and Lows
- Major Highs and Lows (since x [timerange], before/after)
- Direction (up, down, oscillating)
- Barrier horizontal, linear asc/desc
- Kangaroo Tail (up, down)

## Ideas for Systems
- Convergences
- directional convergences (close vs. indicator)
