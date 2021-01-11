# Stage Backtest - stg-backtest

# summarize test results per symbol and day

    SELECT key->symbol, TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC') as day, strategyKey, tradingDirection, SUM(entry) as sumInvest, SUM(revenue) as rev, SUM(revenue)/SUM(entry) as revRatio, SUM(maxPotentialRevenue) as maxPotentialRevenue, SUM(maxPotentialRevenue)/SUM(entry) as maxPotentialRevenueRatio, SUM(durationMS)/1000/60/60/24 as durationOnExit, COUNT(*) as numPositionsOnExit FROM backtestresult_daily WHERE key->symbol='DBK-GY' GROUP BY key->symbol, TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC'), strategyKey, tradingDirection EMIT CHANGES;
    
# filter test results

    SELECT key->symbol, strategyKey, tradingDirection, CONCAT(TIMESTAMPTOSTRING(entryTimestamp, 'yyyy-MM-dd', 'UTC'), ' (', CAST(entryTimestamp AS VARCHAR), ')') as entryDay, entry, CONCAT(TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC'), ' (', CAST(timerangeTimestamp AS VARCHAR), ')') as exitDay, exit, revenue/entry as revRatio FROM backtestresult_daily WHERE revenue/entry < -0.5 EMIT CHANGES LIMIT 10;
    