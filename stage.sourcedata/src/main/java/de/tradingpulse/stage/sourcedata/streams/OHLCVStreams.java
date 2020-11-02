package de.tradingpulse.stage.sourcedata.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.SourceDataStageConstants;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
class OHLCVStreams extends AbstractStreamFactory {
	
	static final String TOPIC_OHLCV_DAILY = SourceDataStageConstants.STAGE_NAME + "-" + "ohlcv_daily";
	static final String TOPIC_OHLCV_WEEKLY = SourceDataStageConstants.STAGE_NAME + "-" + "ohlcv_weekly";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_OHLCV_DAILY,
				TOPIC_OHLCV_WEEKLY
		};
	}
	
	@Singleton
    @Named(TOPIC_OHLCV_DAILY)
    KStream<SymbolTimestampKey, OHLCVRecord> ohlcvDailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_OHLCV_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(OHLCVRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
    }
	
	@Singleton
    @Named(TOPIC_OHLCV_WEEKLY)
    KStream<SymbolTimestampKey, OHLCVRecord> ohlcvWeeklyStream(final ConfiguredStreamBuilder builder) {
    	
		return builder
				.stream(TOPIC_OHLCV_WEEKLY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(OHLCVRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
    }

}
