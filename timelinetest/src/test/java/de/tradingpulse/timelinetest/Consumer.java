package de.tradingpulse.timelinetest;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import de.tradingpulse.common.stream.recordtypes.OHLCVData;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.timelinetest.streams.MinuteStreamFactory;
import de.tradingpulse.timelinetest.streams.SecondsStreamsFactory;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import lombok.Data;

@Data
@KafkaListener(groupId="Consumer")
public class Consumer {
	
	private OHLCVData receivedDataAggMin;
	private OHLCVData receivedDataWinMin;
	
	@Inject
	private SecondsStreamsFactory secondStreamsFactory;
	
	@Inject
	private MinuteStreamFactory minuteStreamsFactory;

	@PreDestroy
	void preDestroy() {
		try {
			secondStreamsFactory.deleteAllTopics();
			minuteStreamsFactory.deleteAllTopics();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	@Topic(MinuteStreamFactory.TOPIC_DATA_AGG_MINUTE)
	public void receiveDataAggMin(@KafkaKey SymbolTimestampKey key, OHLCVData value) {
		this.receivedDataAggMin = value;
	}
	
	@Topic(MinuteStreamFactory.TOPIC_DATA_WINDOWED_MINUTE)
	public void receiveDataWinMin(@KafkaKey SymbolTimestampKey key, OHLCVData value) {
		this.receivedDataWinMin = value;
	}
}
