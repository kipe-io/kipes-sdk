package de.tradingpulse.stage.tradingscreens.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.data.SymbolTimestampKey;
import de.tradingpulse.common.stream.data.TradingScreenData;
import de.tradingpulse.stage.tradingscreens.TradingScreensStageConstants;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
class ImpulseScreenStreams extends AbstractStreamFactory {

	static final String TOPIC_IMPULSE_SCREEN = TradingScreensStageConstants.STAGE_NAME + "-" + "impulse_screen";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_IMPULSE_SCREEN
		};
	}
	
	@Singleton
	@Named(TOPIC_IMPULSE_SCREEN)
	KStream<SymbolTimestampKey, TradingScreenData> impulseScreenStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_IMPULSE_SCREEN, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(TradingScreenData.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}

}
