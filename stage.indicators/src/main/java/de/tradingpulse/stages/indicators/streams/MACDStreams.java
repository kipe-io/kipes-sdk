package de.tradingpulse.stages.indicators.streams;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stages.indicators.IndicatorsStageConstants;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;
import io.kipe.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
class MACDStreams extends AbstractStreamFactory {

	static final String TOPIC_MACD_12_26_9_DAILY = IndicatorsStageConstants.STAGE_NAME + "-" + "macd_12_26_9_daily";
	
	static final String TOPIC_MACD_12_26_9_WEEKLY = IndicatorsStageConstants.STAGE_NAME + "-" + "macd_12_26_9_weekly";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_MACD_12_26_9_DAILY,
				TOPIC_MACD_12_26_9_WEEKLY
		};
	}
	
	@Singleton
	@Named(TOPIC_MACD_12_26_9_DAILY)
	KStream<SymbolTimestampKey, MACDHistogramRecord> macd12269DailyStream(final ConfiguredStreamBuilder builder) {
		return builder
				.stream(TOPIC_MACD_12_26_9_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(MACDHistogramRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton 
	@Named(TOPIC_MACD_12_26_9_WEEKLY)
	KStream<SymbolTimestampKey, MACDHistogramRecord> macd12269WeeklyStream(final ConfiguredStreamBuilder builder) {
		return builder
				.stream(TOPIC_MACD_12_26_9_WEEKLY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(MACDHistogramRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
}
