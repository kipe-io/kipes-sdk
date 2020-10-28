package de.tradingpulse.stage.tradingscreens.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.data.SymbolTimestampKey;
import de.tradingpulse.common.stream.data.TradingScreenData;
import lombok.Getter;

@Singleton
@Getter
public final class TradingScreensStreamsFacade {

	@Inject @Named(ImpulseScreenStreams.TOPIC_IMPULSE_SCREEN)
    private KStream<SymbolTimestampKey, TradingScreenData> impulseScreenStream;
	private final String impulseScreenStreamName = ImpulseScreenStreams.TOPIC_IMPULSE_SCREEN;

}
