package de.tradingpulse.stage.sourcedata.service.processors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import de.tradingpulse.common.stream.recordtypes.OHLCVData;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.utils.TimeUtils;
import de.tradingpulse.stage.sourcedata.SourceDataStageConstants;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;

@Singleton
class OHLCVWeeklyIncrementalProcessor extends AbstractProcessorFactory {
	
	public static final String TOPIC_OHLCV_DAILY_GROUPED_WEEKLY = SourceDataStageConstants.STAGE_NAME + "-" + "ohlcv_daily_grouped_weekly";
	private static final String STORE_WEEKLY_AGGREGATE = "ohlcv_daily_grouped_weekly-store";

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
		.map((dailyKey, dailyData) -> {
			
			SymbolTimestampKey weeklyKey = SymbolTimestampKey.builder()
					.symbol(dailyKey.getSymbol())
					.timestamp(TimeUtils
							.getStartOfWeekTimestampUTC(dailyKey.getTimestamp()))
					.build();
			
			return new KeyValue<>(weeklyKey, dailyData);
		})
		// push to intermediate sink as the subsequent groupByKey would otherwise create a topic by itself
		.through(TOPIC_OHLCV_DAILY_GROUPED_WEEKLY, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(OHLCVData.class)))
		.groupByKey()
		// for the aggregation we strongly require the data flows fifo based on timestamp
		// the initializer returns null as the we take the first value of the group as initial value
		.<OHLCVData>aggregate(() -> null, (key, ohlcvData, aggregateOhlcvData ) -> {
			
			if(aggregateOhlcvData == null) {
				// the data gets the same key as the group (essentially setting the timestamp to week start) 
				ohlcvData.setKey(key);
				return ohlcvData;
			}
			return aggregateOhlcvData.aggregateWith(ohlcvData);
		}, Materialized
				.<SymbolTimestampKey, OHLCVData, KeyValueStore<Bytes,byte[]>>as(STORE_WEEKLY_AGGREGATE) 
				.withKeySerde(jsonSerdeRegistry.getSerde(SymbolTimestampKey.class))
				.withValueSerde(jsonSerdeRegistry.getSerde(OHLCVData.class))
				.withCachingDisabled())	// disabled so that incremental aggregates are available
		.toStream()
		.to(sourceDataStreamsFacade.getOhlcvWeeklyIncrementalStreamName(), Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(OHLCVData.class)));
	}

}
