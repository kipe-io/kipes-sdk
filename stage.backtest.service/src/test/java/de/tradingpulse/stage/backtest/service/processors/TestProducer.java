package de.tradingpulse.stage.backtest.service.processors;

import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaClient
public interface TestProducer {

	@Topic(TradingScreensStreamsFacade.TOPIC_SIGNAL_DAILY)
	void sendSignalRecord(@KafkaKey SymbolTimestampKey key, SignalRecord signalRecord);
}
