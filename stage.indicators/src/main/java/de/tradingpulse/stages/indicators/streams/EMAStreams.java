package de.tradingpulse.stages.indicators.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.DoubleData;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stages.indicators.IndicatorsStageConstants;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
class EMAStreams extends AbstractStreamFactory {

	static final String TOPIC_EMA_13_DAILY = IndicatorsStageConstants.STAGE_NAME + "-" + "ema_13_daily";
	static final String TOPIC_EMA_26_DAILY = IndicatorsStageConstants.STAGE_NAME + "-" + "ema_26_daily";

	static final String TOPIC_EMA_13_WEEKLY_INCREMENTAL = IndicatorsStageConstants.STAGE_NAME + "-" + "ema_13_weekly_incremental";
	static final String TOPIC_EMA_26_WEEKLY_INCREMENTAL = IndicatorsStageConstants.STAGE_NAME + "-" + "ema_26_weekly_incremental";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_EMA_13_DAILY,
				TOPIC_EMA_26_DAILY,
				TOPIC_EMA_13_WEEKLY_INCREMENTAL,
				TOPIC_EMA_26_WEEKLY_INCREMENTAL
		};
	}
	
	@Singleton
	@Named(TOPIC_EMA_13_DAILY)
	KStream<SymbolTimestampKey, DoubleData> ema13DailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_EMA_13_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(DoubleData.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_EMA_26_DAILY)
	KStream<SymbolTimestampKey, DoubleData> ema26DailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_EMA_26_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(DoubleData.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_EMA_13_WEEKLY_INCREMENTAL)
	KStream<SymbolTimestampKey, DoubleData> ema13WeeklyIncrementalStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_EMA_13_WEEKLY_INCREMENTAL, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(DoubleData.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_EMA_26_WEEKLY_INCREMENTAL)
	KStream<SymbolTimestampKey, DoubleData> ema26WeeklyIncrementalStream(final ConfiguredStreamBuilder builder)	{
		
		return builder
				.stream(TOPIC_EMA_26_WEEKLY_INCREMENTAL, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(DoubleData.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
}
