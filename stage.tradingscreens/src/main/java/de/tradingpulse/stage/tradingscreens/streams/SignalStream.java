package de.tradingpulse.stage.tradingscreens.streams;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.TradingScreensStageConstants;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
class SignalStream extends AbstractStreamFactory {
	
	static final String TOPIC_SIGNAL_DAILY = TradingScreensStageConstants.STAGE_NAME + "-" + "signal_daily";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_SIGNAL_DAILY
		};
	}
	
	@Singleton
    @Named(TOPIC_SIGNAL_DAILY)
    KStream<SymbolTimestampKey, SignalRecord> signalDailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_SIGNAL_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(SignalRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
    }
}
