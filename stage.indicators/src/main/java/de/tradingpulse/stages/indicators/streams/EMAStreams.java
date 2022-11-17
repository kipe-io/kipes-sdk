package de.tradingpulse.stages.indicators.streams;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stages.indicators.IndicatorsStageConstants;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
class EMAStreams extends AbstractStreamFactory {

	static final String TOPIC_EMA_13_DAILY = IndicatorsStageConstants.STAGE_NAME + "-" + "ema_13_daily";
	static final String TOPIC_EMA_26_DAILY = IndicatorsStageConstants.STAGE_NAME + "-" + "ema_26_daily";

	static final String TOPIC_EMA_13_WEEKLY = IndicatorsStageConstants.STAGE_NAME + "-" + "ema_13_weekly";
	static final String TOPIC_EMA_26_WEEKLY = IndicatorsStageConstants.STAGE_NAME + "-" + "ema_26_weekly";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_EMA_13_DAILY,
				TOPIC_EMA_26_DAILY,
				TOPIC_EMA_13_WEEKLY,
				TOPIC_EMA_26_WEEKLY
		};
	}
	
	@Singleton
	@Named(TOPIC_EMA_13_DAILY)
	KStream<SymbolTimestampKey, DoubleRecord> ema13DailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_EMA_13_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(DoubleRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_EMA_26_DAILY)
	KStream<SymbolTimestampKey, DoubleRecord> ema26DailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_EMA_26_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(DoubleRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_EMA_13_WEEKLY)
	KStream<SymbolTimestampKey, DoubleRecord> ema13WeeklyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_EMA_13_WEEKLY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(DoubleRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_EMA_26_WEEKLY)
	KStream<SymbolTimestampKey, DoubleRecord> ema26WeeklyStream(final ConfiguredStreamBuilder builder)	{
		
		return builder
				.stream(TOPIC_EMA_26_WEEKLY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(DoubleRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
}
