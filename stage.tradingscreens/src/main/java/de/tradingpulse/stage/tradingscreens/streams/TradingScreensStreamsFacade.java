package de.tradingpulse.stage.tradingscreens.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.data.SwingTradingScreenData;
import lombok.Getter;

@Singleton
@Getter
public final class TradingScreensStreamsFacade {

	@Inject @Named(ImpulseScreenStreams.TOPIC_SWING_TRADING_SCREEN)
    private KStream<SymbolTimestampKey, SwingTradingScreenData> swingTradingScreenStream;
	private final String swingTradingScreenStreamName = ImpulseScreenStreams.TOPIC_SWING_TRADING_SCREEN;

}
