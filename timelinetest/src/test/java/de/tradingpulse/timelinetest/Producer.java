package de.tradingpulse.timelinetest;

import de.tradingpulse.common.stream.data.OHLCVData;
import de.tradingpulse.common.stream.data.SymbolTimestampKey;
import de.tradingpulse.timelinetest.streams.SecondsStreamsFactory;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaClient(id="timelinetest")
public interface Producer {
	
	@Topic(SecondsStreamsFactory.TOPIC_DATA_15_SEC)
	void send(@KafkaKey SymbolTimestampKey key, OHLCVData value);
}
