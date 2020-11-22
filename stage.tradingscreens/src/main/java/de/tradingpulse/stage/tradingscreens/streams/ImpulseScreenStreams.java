package de.tradingpulse.stage.tradingscreens.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.TradingScreensStageConstants;
import de.tradingpulse.stage.tradingscreens.data.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.data.SignalRecord;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
class ImpulseScreenStreams extends AbstractStreamFactory {

	static final String TOPIC_IMPULSE_TRADING_SCREEN = TradingScreensStageConstants.STAGE_NAME + "-" + "impulse_trading_screen";
	static final String TOPIC_IMPULSE_MOMENTUM_SIGNALS = TradingScreensStageConstants.STAGE_NAME + "-" + "impulse_momentum_signals";
	static final String TOPIC_IMPULSE_POTENTIAL_SIGNALS = TradingScreensStageConstants.STAGE_NAME + "-" + "impulse_potential_signals";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_IMPULSE_TRADING_SCREEN,
				TOPIC_IMPULSE_MOMENTUM_SIGNALS,
				TOPIC_IMPULSE_POTENTIAL_SIGNALS
		};
	}
	
	@Singleton
	@Named(TOPIC_IMPULSE_TRADING_SCREEN)
	KStream<SymbolTimestampKey, ImpulseTradingScreenRecord> impulseTradingScreenStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_IMPULSE_TRADING_SCREEN, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(ImpulseTradingScreenRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_IMPULSE_MOMENTUM_SIGNALS)
	KStream<SymbolTimestampKey, SignalRecord> impulseMomentumSignalsStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_IMPULSE_MOMENTUM_SIGNALS, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(SignalRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_IMPULSE_POTENTIAL_SIGNALS)
	KStream<SymbolTimestampKey, SignalRecord> impulsePotentialSignalsStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_IMPULSE_POTENTIAL_SIGNALS, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(SignalRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}

}
