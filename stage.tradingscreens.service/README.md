# Stage Tradingscreens - stg-tradingscreens

## Show last entry signals

    $ kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic stg-tradingscreens-swing_trading_screen --from-beginning | jq -c '.| {"symbol":.key.symbol, "ts":.key.timestamp, "lld":.longRangeImpulseRecord.lastTradingDirection, "lch":.longRangeImpulseRecord.changeTradingDirection, "lcd":.longRangeImpulseRecord.tradingDirection, "sld":.shortRangeImpulseRecord.lastTradingDirection, "sch":.shortRangeImpulseRecord.changeTradingDirection, "scd":.shortRangeImpulseRecord.tradingDirection, "entry":.entrySignal }' -
    
