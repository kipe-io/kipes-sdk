package de.tradingpulse.stages.indicators.streams;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stages.indicators.IndicatorsStageConstants;
import de.tradingpulse.stages.indicators.recordtypes.SSTOCRecord;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
public class SSTOCStreams extends AbstractStreamFactory {

	static final String TOPIC_SSTOC_5_5_3_DAILY = IndicatorsStageConstants.STAGE_NAME + "-" + "sstoc_5_5_3_daily";
	static final String TOPIC_SSTOC_5_5_3_WEEKLY = IndicatorsStageConstants.STAGE_NAME + "-" + "sstoc_5_5_3_weekly";
	

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_SSTOC_5_5_3_DAILY,
				TOPIC_SSTOC_5_5_3_WEEKLY
		};
	}
	
	@Singleton
	@Named(TOPIC_SSTOC_5_5_3_DAILY)
	KStream<SymbolTimestampKey, SSTOCRecord> sstoc553DailyStream(final ConfiguredStreamBuilder builder) {
		return builder
				.stream(TOPIC_SSTOC_5_5_3_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(SSTOCRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton 
	@Named(TOPIC_SSTOC_5_5_3_WEEKLY)
	KStream<SymbolTimestampKey, SSTOCRecord> sstoc553WeeklyStream(final ConfiguredStreamBuilder builder) {
		return builder
				.stream(TOPIC_SSTOC_5_5_3_WEEKLY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(SSTOCRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
}
