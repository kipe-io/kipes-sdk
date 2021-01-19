# Stage Backtest - stg-backtest

# summarize all tests

    SELECT strategyKey, tradingDirection, SUM(entry) as sumInvest, SUM(revenue) as rev, SUM(revenue)/SUM(entry) as revRatio, SUM(maxPotentialRevenue) as maxPotentialRevenue, SUM(maxPotentialRevenue)/SUM(entry) as maxPotentialRevenueRatio, SUM(durationMS)/1000/60/60/24 as durationOnExit, COUNT(*) as numPositionsOnExit FROM backtestresult_daily WHERE true GROUP BY strategyKey, tradingDirection EMIT CHANGES;

# summarize test results per symbol and day

    SELECT key->symbol, TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC') as day, strategyKey, tradingDirection, SUM(entry) as sumInvest, SUM(revenue) as rev, SUM(revenue)/SUM(entry) as revRatio, SUM(maxPotentialRevenue) as maxPotentialRevenue, SUM(maxPotentialRevenue)/SUM(entry) as maxPotentialRevenueRatio, SUM(durationMS)/1000/60/60/24 as durationOnExit, COUNT(*) as numPositionsOnExit FROM backtestresult_daily WHERE key->symbol='DBK-GY' GROUP BY key->symbol, TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC'), strategyKey, tradingDirection EMIT CHANGES;
    
# filter test results

    SELECT key->symbol AS backtest_symbol, strategyKey, tradingDirection, CONCAT(TIMESTAMPTOSTRING(entryTimestamp, 'yyyy-MM-dd', 'UTC'), ' (', CAST(entryTimestamp AS VARCHAR), ')') as entryDay, entry, CONCAT(TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC'), ' (', CAST(timerangeTimestamp AS VARCHAR), ')') as exitDay, exit, revenue/entry as revRatio FROM backtestresult_daily WHERE revenue/entry < -0.5 EMIT CHANGES LIMIT 10;

# signal execution for a specific symbol and time range

	SELECT key->symbol AS signal_exec_symbol, CONCAT(TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC'), ' (', CAST(timerangeTimestamp AS VARCHAR), ')') as day, CONCAT(TIMESTAMPTOSTRING(signalRecord->timerangeTimestamp, 'yyyy-MM-dd', 'UTC'), ' (', CAST(signalRecord->timerangeTimestamp AS VARCHAR), ')') as daySignal, CONCAT(signalRecord->strategyKey, ':', signalRecord->signalType) as signal, CONCAT(TIMESTAMPTOSTRING(ohlcvRecord->timerangeTimestamp, 'yyyy-MM-dd', 'UTC'), ' (', CAST(ohlcvRecord->timerangeTimestamp AS VARCHAR), ')') as dayOHLCV FROM signal_execution_daily WHERE key->symbol='TPR' AND timerangeTimestamp >= 1582243200000 - 3*86400000 AND timerangeTimestamp <= 1582848000000 + 2*86400000 EMIT CHANGES;

# signals for a specific symbol and time range

	SELECT key->symbol AS signal_symbol, CONCAT(TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC'), ' (', CAST(timerangeTimestamp AS VARCHAR), ')') as day, strategyKey, signalType FROM signal_daily WHERE key->symbol='TPR' AND timerangeTimestamp >= 1582243200000 - 3*86400000 AND timerangeTimestamp <= 1582848000000 + 2*86400000 EMIT CHANGES;

# impulse screen signals for a specific symbol and time range

	SELECT key->symbol AS impulse_screen_symbol, CONCAT(TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC'), ' (', CAST(timerangeTimestamp AS VARCHAR), ')') as day, entrySignal, exitSignal, longrangeImpulseRecord->lastTradingDirection AS longLast, longrangeImpulseRecord->tradingDirection AS longCurrent, shortrangeImpulseRecord->lastTradingDirection AS shortLast, shortrangeImpulseRecord->tradingDirection AS shortCurrent FROM impulse_trading_screen WHERE key->symbol='ZM' AND timerangeTimestamp >= 1580169600000 - 5*86400000 AND timerangeTimestamp <= 1581292800000 + 1*86400000 EMIT CHANGES;



# ohlcv for a specific symbol and time range

	SELECT key->symbol AS ohlcv_symbol, CONCAT(TIMESTAMPTOSTRING(timerangeTimestamp, 'yyyy-MM-dd', 'UTC'), ' (', CAST(timerangeTimestamp AS VARCHAR), ')') as day, open, high, low, close, volume FROM ohlcv_daily WHERE key->symbol='TPR' AND timerangeTimestamp >= 1582243200000 - 86400000 AND timerangeTimestamp <= 1582848000000 EMIT CHANGES;