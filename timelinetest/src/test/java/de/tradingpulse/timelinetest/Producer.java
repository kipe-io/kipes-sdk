package de.tradingpulse.timelinetest;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.timelinetest.streams.SecondsStreamsFactory;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaClient(id="timelinetest")
public interface Producer {
	
	@Topic(SecondsStreamsFactory.TOPIC_DATA_15_SEC)
	void send(@KafkaKey SymbolTimestampKey key, OHLCVRecord value);
}
