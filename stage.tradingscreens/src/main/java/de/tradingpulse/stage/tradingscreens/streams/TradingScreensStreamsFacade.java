package de.tradingpulse.stage.tradingscreens.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseSignalRecord;
import lombok.Getter;

@Singleton
@Getter
public final class TradingScreensStreamsFacade {

	@Inject @Named(ImpulseScreenStreams.TOPIC_IMPULSE_TRADING_SCREEN)
    private KStream<SymbolTimestampKey, ImpulseTradingScreenRecord> impulseTradingScreenStream;
	private final String impulseTradingScreenStreamName = ImpulseScreenStreams.TOPIC_IMPULSE_TRADING_SCREEN;

	@Inject @Named(ImpulseScreenStreams.TOPIC_IMPULSE_MOMENTUM_SIGNALS)
    private KStream<SymbolTimestampKey, ImpulseSignalRecord> impulseMomentumSignalsStream;
	private final String impulseMomentumSignalsStreamName = ImpulseScreenStreams.TOPIC_IMPULSE_MOMENTUM_SIGNALS;

	@Inject @Named(ImpulseScreenStreams.TOPIC_IMPULSE_POTENTIAL_SIGNALS)
    private KStream<SymbolTimestampKey, ImpulseSignalRecord> impulsePotentialSignalsStream;
	private final String impulsePotentialSignalsStreamName = ImpulseScreenStreams.TOPIC_IMPULSE_POTENTIAL_SIGNALS;

}
