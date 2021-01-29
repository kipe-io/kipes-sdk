package de.tradingpulse.stage.tradingscreens.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.TradingScreensStageConstants;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
public class TrendsValueStream extends AbstractStreamFactory {
	
	static final String TOPIC_TRENDS_VALUE = TradingScreensStageConstants.STAGE_NAME + "-" + "trends_value";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_TRENDS_VALUE
		};
	}
	
	@Singleton
    @Named(TOPIC_TRENDS_VALUE)
    KStream<SymbolTimestampKey, GenericRecord> trendsValueStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_TRENDS_VALUE, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(GenericRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
    }

}
