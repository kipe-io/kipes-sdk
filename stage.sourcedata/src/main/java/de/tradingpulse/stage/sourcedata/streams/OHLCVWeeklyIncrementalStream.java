package de.tradingpulse.stage.sourcedata.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.OHLCVData;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.SourceDataStageConstants;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
class OHLCVWeeklyIncrementalStream extends AbstractStreamFactory {
	
	static final String TOPIC_OHLCV_WEEKLY_INCREMENTAL = SourceDataStageConstants.STAGE_NAME + "-" + "ohlcv_weekly_incremental";
	
	@Inject @Named(OHLCVDailyStream.TOPIC_OHLCV_DAILY)
	private KStream<SymbolTimestampKey, OHLCVData> ohlcvDailyStream;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_OHLCV_WEEKLY_INCREMENTAL
		};
	}
	
	@Singleton
    @Named(TOPIC_OHLCV_WEEKLY_INCREMENTAL)
    KStream<SymbolTimestampKey, OHLCVData> ohlcvWeeklyStream(final ConfiguredStreamBuilder builder) {
    	
		return builder
				.stream(TOPIC_OHLCV_WEEKLY_INCREMENTAL, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(OHLCVData.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
    }

}
