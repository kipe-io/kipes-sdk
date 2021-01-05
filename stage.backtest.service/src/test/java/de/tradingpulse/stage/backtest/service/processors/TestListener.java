package de.tradingpulse.stage.backtest.service.processors;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.recordtypes.SignalExecutionRecord;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import javax.inject.Singleton;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;

@Singleton
@KafkaListener(groupId = "test", clientId = "test-listener")
public class TestListener {
	
    private BlockingQueue<ConsumerRecord<SymbolTimestampKey, SignalRecord>> signalRecords = new LinkedBlockingDeque<>();

    @Topic(TradingScreensStreamsFacade.TOPIC_SIGNAL_DAILY)
    public void eventOccurred(ConsumerRecord<SymbolTimestampKey, SignalRecord> record) {
    	signalRecords.add(record);
    }

    public BlockingQueue<ConsumerRecord<SymbolTimestampKey, SignalRecord>> getSignalRecords() {
        return this.signalRecords;
    }

}
