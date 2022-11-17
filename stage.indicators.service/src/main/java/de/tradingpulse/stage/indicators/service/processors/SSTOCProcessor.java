package de.tradingpulse.stage.indicators.service.processors;

import static de.tradingpulse.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import de.tradingpulse.common.stream.aggregates.IncrementalAggregate;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import de.tradingpulse.stages.indicators.recordtypes.SSTOCRecord;
import de.tradingpulse.stages.indicators.streams.IndicatorsStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class SSTOCProcessor extends AbstractProcessorFactory {

	@Inject
	private SourceDataStreamsFacade sourceDataStreamsFacade;

	@Inject
	private IndicatorsStreamsFacade indicatorsStreamsFacade;
	
	@Inject
	private ConfiguredStreamBuilder builder;
	
	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected void initProcessors() throws InterruptedException, ExecutionException {
		// SSTOC(5,5,3) daily
		initSSTOCStream(
				indicatorsStreamsFacade.getSstoc553DailyStreamName(),
				5,5,3,
				sourceDataStreamsFacade.getOhlcvDailyStream());
		
		// SSTOC(5,5,3) weekly
		initSSTOCStream(
				indicatorsStreamsFacade.getSstoc553WeeklyStreamName(),
				5,5,3,
				sourceDataStreamsFacade.getOhlcvWeeklyStream());
		
	}

	void initSSTOCStream(
			String topicName, 
			int n, int p1, int p2,
			KStream<SymbolTimestampKey, OHLCVRecord> sourceStream)
	{
		final String storeName = getProcessorStoreTopicName(topicName);
		// create store
		@SuppressWarnings("rawtypes")
		StoreBuilder<KeyValueStore<String,IncrementalAggregate>> keyValueStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName),
						jsonSerdeRegistry.getSerde(String.class),
						jsonSerdeRegistry.getSerde(IncrementalAggregate.class));
		
		// register store
		builder.addStateStore(keyValueStoreBuilder);
		
		
		// create topology
		sourceStream.transform(
				() -> new SSTOCTransformer(storeName, n, p1, p2), 
				storeName)
		.to(topicName, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(SSTOCRecord.class)));
	}
}
