package de.tradingpulse.stage.systems.streams;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.SystemsStageConstants;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
class ImpulseStreams extends AbstractStreamFactory {

	static final String TOPIC_IMPULSE_DAILY = SystemsStageConstants.STAGE_NAME + "-" + "impulse_daily";
	static final String TOPIC_IMPULSE_WEEKLY = SystemsStageConstants.STAGE_NAME + "-" + "impulse_weekly";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_IMPULSE_DAILY,
				TOPIC_IMPULSE_WEEKLY
		};
	}
	
	@Singleton
	@Named(TOPIC_IMPULSE_DAILY)
	KStream<SymbolTimestampKey, ImpulseRecord> impulseDailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_IMPULSE_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(ImpulseRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_IMPULSE_WEEKLY)
	KStream<SymbolTimestampKey, ImpulseRecord> impulseWeeklyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_IMPULSE_WEEKLY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(ImpulseRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
}
