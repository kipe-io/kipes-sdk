# Stage Tradingscreens - stg-tradingscreens

## Show last entry signals

    $ kafka-console-consumer.sh --bootstrap-server pkc-lq8v7.eu-central-1.aws.confluent.cloud:9092 --consumer.config ~/.kafka/client.config.confluent --topic stg-tradingscreens-swing_trading_screen --from-beginning | jq -c '.| {"symbol":.key.symbol, "ts":.key.timestamp, "lld":.longRangeImpulseRecord.lastTradingDirection, "lch":.longRangeImpulseRecord.changeTradingDirection, "lcd":.longRangeImpulseRecord.tradingDirection, "sld":.shortRangeImpulseRecord.lastTradingDirection, "sch":.shortRangeImpulseRecord.changeTradingDirection, "scd":.shortRangeImpulseRecord.tradingDirection, "entry":.entrySignal }' -
    
