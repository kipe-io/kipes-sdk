package de.tradingpulse.stage.systems.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.data.ImpulseData;
import de.tradingpulse.common.stream.data.SymbolTimestampKey;
import de.tradingpulse.stage.systems.SystemsStageConstants;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
class ImpulseStreams extends AbstractStreamFactory {

	static final String TOPIC_IMPULSE_DAILY = SystemsStageConstants.STAGE_NAME + "-" + "impulse_daily";
	static final String TOPIC_IMPULSE_WEEKLY_INCREMENTAL = SystemsStageConstants.STAGE_NAME + "-" + "impulse_weekly_incremental";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_IMPULSE_DAILY,
				TOPIC_IMPULSE_WEEKLY_INCREMENTAL
		};
	}
	
	@Singleton
	@Named(TOPIC_IMPULSE_DAILY)
	KStream<SymbolTimestampKey, ImpulseData> impulseDailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_IMPULSE_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(ImpulseData.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_IMPULSE_WEEKLY_INCREMENTAL)
	KStream<SymbolTimestampKey, ImpulseData> impulseWeeklyIncrementalStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_IMPULSE_WEEKLY_INCREMENTAL, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(ImpulseData.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
}
