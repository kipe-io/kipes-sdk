package de.tradingpulse.stage.tradingscreens.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.TradingScreensStageConstants;
import de.tradingpulse.stage.tradingscreens.data.SwingTradingScreenRecord;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
class ImpulseScreenStreams extends AbstractStreamFactory {

	static final String TOPIC_SWING_TRADING_SCREEN = TradingScreensStageConstants.STAGE_NAME + "-" + "swing_trading_screen";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_SWING_TRADING_SCREEN
		};
	}
	
	@Singleton
	@Named(TOPIC_SWING_TRADING_SCREEN)
	KStream<SymbolTimestampKey, SwingTradingScreenRecord> swingTradingScreenStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_SWING_TRADING_SCREEN, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(SwingTradingScreenRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}

}
