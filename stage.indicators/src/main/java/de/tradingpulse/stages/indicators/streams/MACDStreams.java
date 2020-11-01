package de.tradingpulse.stages.indicators.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.MACDHistogramData;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stages.indicators.IndicatorsStageConstants;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
class MACDStreams extends AbstractStreamFactory {

	static final String TOPIC_MACD_12_26_9_DAILY = IndicatorsStageConstants.STAGE_NAME + "-" + "macd_12_26_9_daily";
	
	static final String TOPIC_MACD_12_26_9_WEEKLY_INCREMENTAL = IndicatorsStageConstants.STAGE_NAME + "-" + "macd_12_26_9_weekly_incremental";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_MACD_12_26_9_DAILY,
				TOPIC_MACD_12_26_9_WEEKLY_INCREMENTAL
		};
	}
	
	@Singleton
	@Named(TOPIC_MACD_12_26_9_DAILY)
	KStream<SymbolTimestampKey, MACDHistogramData> macd12269DailyStream(final ConfiguredStreamBuilder builder) {
		return builder
				.stream(TOPIC_MACD_12_26_9_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(MACDHistogramData.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton 
	@Named(TOPIC_MACD_12_26_9_WEEKLY_INCREMENTAL)
	KStream<SymbolTimestampKey, MACDHistogramData> macd12269WeeklyIncrementalStream(final ConfiguredStreamBuilder builder) {
		return builder
				.stream(TOPIC_MACD_12_26_9_WEEKLY_INCREMENTAL, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(MACDHistogramData.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
}
