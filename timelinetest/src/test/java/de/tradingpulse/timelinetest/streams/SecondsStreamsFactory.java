package de.tradingpulse.timelinetest.streams;

import java.util.concurrent.ExecutionException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.OHLCVRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
public class SecondsStreamsFactory extends AbstractStreamFactory {

	public static final String TOPIC_DATA_15_SEC = "timelinetest-data_15_sec";
	
	@Inject
	private JsonSerdeRegistry serdeRegistry;
	
	@Override
	protected String[] getTopicNames() {
		return new String[]{ TOPIC_DATA_15_SEC };
	}
	
	@Singleton @Named(TOPIC_DATA_15_SEC)
	KStream<SymbolTimestampKey, OHLCVRecord> data15SecStream(final ConfiguredStreamBuilder builder)
	throws InterruptedException, ExecutionException
	{
		return builder
				.stream(TOPIC_DATA_15_SEC, Consumed.with(
						serdeRegistry.getSerde(SymbolTimestampKey.class), 
						serdeRegistry.getSerde(OHLCVRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.LATEST));
	}

}
