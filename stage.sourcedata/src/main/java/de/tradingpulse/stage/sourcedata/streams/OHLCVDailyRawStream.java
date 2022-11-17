package de.tradingpulse.stage.sourcedata.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.stage.sourcedata.SourceDataStageConstants;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRawRecord;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
class OHLCVDailyRawStream extends AbstractStreamFactory {

	static final String TOPIC_OHLCV_DAILY_RAW = SourceDataStageConstants.STAGE_NAME + "-" + "ohlcv_daily_raw";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_OHLCV_DAILY_RAW
		};
	}
	
	@Singleton
    @Named(TOPIC_OHLCV_DAILY_RAW)
    KStream<String, OHLCVRawRecord> ohlcvDailyRawStream(final ConfiguredStreamBuilder builder) {
		
		return builder.stream(
        		TOPIC_OHLCV_DAILY_RAW,
        		Consumed
        		.with(Serdes.String(), jsonSerdeRegistry.getSerde(OHLCVRawRecord.class))
        		.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
    }
}
