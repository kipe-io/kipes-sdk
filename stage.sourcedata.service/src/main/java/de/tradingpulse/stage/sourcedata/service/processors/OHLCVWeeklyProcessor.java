package de.tradingpulse.stage.sourcedata.service.processors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import de.tradingpulse.common.stream.recordtypes.OHLCVRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TimeRange;
import de.tradingpulse.common.utils.TimeUtils;
import de.tradingpulse.stage.sourcedata.SourceDataStageConstants;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;

@Singleton
class OHLCVWeeklyProcessor extends AbstractProcessorFactory {
	
	static final String TOPIC_OHLCV_DAILY_GROUPED_WEEKLY = SourceDataStageConstants.STAGE_NAME + "-" + "ohlcv_daily_grouped_weekly";

	@Inject
	private SourceDataStreamsFacade sourceDataStreamsFacade;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_OHLCV_DAILY_GROUPED_WEEKLY
		};
	}

	@Override
	protected void initProcessors() {
		
		sourceDataStreamsFacade.getOhlcvDailyStream()
		// map daily keys to weekly keys to prepare grouping
		.map((dailyKey, dailyRecord) -> {
			
			SymbolTimestampKey weeklyKey = SymbolTimestampKey.builder()
					.symbol(dailyKey.getSymbol())
					.timestamp(TimeUtils
							.getStartOfWeekTimestampUTC(dailyKey.getTimestamp()))
					.build();
			
			return new KeyValue<>(weeklyKey, dailyRecord);
		})
		// push to intermediate sink as the subsequent groupByKey would otherwise create a topic by itself
		.through(TOPIC_OHLCV_DAILY_GROUPED_WEEKLY, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(OHLCVRecord.class)))
		.groupByKey()
		// for the aggregation we strongly require the records flows fifo based on timestamp
		// the initializer returns null as the we take the first value of the group as initial value
		.<OHLCVRecord>aggregate(() -> null, (key, ohlcvRecord, aggregateOhlcvRecord ) -> {
			
			if(aggregateOhlcvRecord == null) {
				// the record gets a new time range interpretation 
				ohlcvRecord.setTimeRange(TimeRange.WEEK);
				return ohlcvRecord;
			}
			return aggregateOhlcvRecord.aggregateWith(ohlcvRecord);
		}, Materialized
				.<SymbolTimestampKey, OHLCVRecord, KeyValueStore<Bytes,byte[]>>as(getProcessorStoreTopicName(TOPIC_OHLCV_DAILY_GROUPED_WEEKLY)) 
				.withKeySerde(jsonSerdeRegistry.getSerde(SymbolTimestampKey.class))
				.withValueSerde(jsonSerdeRegistry.getSerde(OHLCVRecord.class))
				.withCachingDisabled())	// disabled so that incremental aggregates are available
		.toStream()
		.to(sourceDataStreamsFacade.getOhlcvWeeklyStreamName(), Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(OHLCVRecord.class)));
	}

}
