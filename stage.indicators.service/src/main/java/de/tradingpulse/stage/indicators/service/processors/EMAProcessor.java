package de.tradingpulse.stage.indicators.service.processors;

import static io.kipe.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;

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
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;
import de.tradingpulse.stages.indicators.streams.IndicatorsStreamsFacade;
import io.kipe.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
class EMAProcessor extends AbstractProcessorFactory {

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
		// EMA(13) daily
		initEMAStream(
				indicatorsStreamsFacade.getEma13DailyStreamName(), 
				13, 
				sourceDataStreamsFacade.getOhlcvDailyStream());
		
		// EMA(26) daily
		initEMAStream(
				indicatorsStreamsFacade.getEma26DailyStreamName(), 
				26, 
				sourceDataStreamsFacade.getOhlcvDailyStream());
		
		// EMA(13) weekly incremental
		initEMAStream(
				indicatorsStreamsFacade.getEma13WeeklyStreamName(), 
				13, 
				sourceDataStreamsFacade.getOhlcvWeeklyStream());
		
		// EMA(26) weekly incremental
		initEMAStream(
				indicatorsStreamsFacade.getEma26WeeklyStreamName(), 
				26, 
				sourceDataStreamsFacade.getOhlcvWeeklyStream());
	}
	
	void initEMAStream(
			final String topicName,
			final int numObservations,
			final KStream<SymbolTimestampKey, OHLCVRecord> sourceStream 
	) {
		
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
				() -> new EMATransformer(storeName, numObservations),
				storeName)
		.to(topicName, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(DoubleRecord.class)));
	}
	
	// ------------------------------------------------------------------------
	// inner classes
	// ------------------------------------------------------------------------

}
