package de.tradingpulse.stage.indicators.service.processors;

import java.util.concurrent.ExecutionException;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import de.tradingpulse.common.stream.aggregates.IncrementalAggregate;
import de.tradingpulse.common.stream.recordtypes.DoubleData;
import de.tradingpulse.common.stream.recordtypes.OHLCVRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import de.tradingpulse.stages.indicators.streams.IndicatorsStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class EMAIncrementalProcessor extends AbstractProcessorFactory {

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
		initEMAIncrementalStream(
				indicatorsStreamsFacade.getEma13DailyStreamName(), 
				13, 
				sourceDataStreamsFacade.getOhlcvDailyStream());
		
		// EMA(26) daily
		initEMAIncrementalStream(
				indicatorsStreamsFacade.getEma26DailyStreamName(), 
				26, 
				sourceDataStreamsFacade.getOhlcvDailyStream());
		
		// EMA(13) weekly incremental
		initEMAIncrementalStream(
				indicatorsStreamsFacade.getEma13WeeklyIncrementalStreamName(), 
				13, 
				sourceDataStreamsFacade.getOhlcvWeeklyStream());
		
		// EMA(26) weekly incremental
		initEMAIncrementalStream(
				indicatorsStreamsFacade.getEma26WeeklyIncrementalStreamName(), 
				26, 
				sourceDataStreamsFacade.getOhlcvWeeklyStream());
	}
	
	void initEMAIncrementalStream(
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
				() -> new IncrementalEMATransformer(storeName, numObservations),
				storeName)
		.to(topicName, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(DoubleData.class)));
	}
	
	// ------------------------------------------------------------------------
	// inner classes
	// ------------------------------------------------------------------------

}
