package de.tradingpulse.stage.tradingscreens.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import lombok.Getter;

@Singleton
@Getter
public class TradingScreensStreamsFacade {

	@Inject @Named(ImpulseScreenStreams.TOPIC_IMPULSE_TRADING_SCREEN)
    private KStream<SymbolTimestampKey, ImpulseTradingScreenRecord> impulseTradingScreenStream;
	private final String impulseTradingScreenStreamName = ImpulseScreenStreams.TOPIC_IMPULSE_TRADING_SCREEN;

	@Inject @Named(SignalStream.TOPIC_SIGNAL_DAILY)
    private KStream<SymbolTimestampKey, SignalRecord> signalDailyStream;
	public static final String TOPIC_SIGNAL_DAILY = SignalStream.TOPIC_SIGNAL_DAILY;
}
