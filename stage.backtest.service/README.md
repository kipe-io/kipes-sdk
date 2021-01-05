# Stage Backtest - stg-backtest

# summarize test results per symbol and day

    SELECT key->symbol, TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC') as month, strategyKey, tradingDirection, SUM(entryValue) as sumInvest, SUM(revenue) as rev, SUM(revenue)/SUM(entryValue) as revRatio, SUM(durationMS)/1000/60/60/24 as durationOnExit, COUNT(*) as numPositionsOnExit FROM backtestresult_daily WHERE key->symbol='DBK-GY' GROUP BY key->symbol, TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC'), strategyKey, tradingDirection EMIT CHANGES;