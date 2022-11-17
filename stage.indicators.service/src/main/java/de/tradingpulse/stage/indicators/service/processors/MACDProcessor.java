package de.tradingpulse.stage.indicators.service.processors;

import static de.tradingpulse.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import de.tradingpulse.common.stream.aggregates.IncrementalAggregate;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;
import de.tradingpulse.stages.indicators.streams.IndicatorsStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
class MACDProcessor extends AbstractProcessorFactory {

	@Inject
	private SourceDataStreamsFacade sourceDataStreamsFacade;

	@Inject
	private IndicatorsStreamsFacade indicatorsStreamsFacade;
	
	@Inject
	private ConfiguredStreamBuilder builder;
	
	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() throws Exception {
		// MACD daily 12,26,9
		initMACDHistogramStream(
				indicatorsStreamsFacade.getMacd12269DailyStreamName(), 
				12, 26, 9, 
				sourceDataStreamsFacade.getOhlcvDailyStream(), 
				builder);
		// MACD weekly incremental 12,26,9
		initMACDHistogramStream(
				indicatorsStreamsFacade.getMacd12269WeeklyStreamName(), 
				12, 26, 9, 
				sourceDataStreamsFacade.getOhlcvWeeklyStream(), 
				builder);
	}
	
	void initMACDHistogramStream(
			final String topicName,
			final int fastPeriod,
			final int slowPeriod,
			final int signalPeriod,
			final KStream<SymbolTimestampKey, OHLCVRecord> sourceStream, 
			final ConfiguredStreamBuilder builder
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
				() -> new MACDTransformer(storeName, fastPeriod, slowPeriod, signalPeriod), 
				storeName)
		.to(topicName, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(MACDHistogramRecord.class)));
	}
}
