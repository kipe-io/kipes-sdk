package de.tradingpulse.stage.sourcedata.service.processors;

import static io.kipe.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import io.kipe.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
class OHLCVDailyProcessor extends AbstractProcessorFactory {

	@Inject
	private SourceDataStreamsFacade sourceDataStreamsFacade;
	
	@Inject
	private ConfiguredStreamBuilder builder;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected void initProcessors() throws InterruptedException, ExecutionException {
		
		// setup raw keyed topic
		String ohlcvDailyRawKeyedTopicName = sourceDataStreamsFacade.getOhlcvDailyRawStreamName() + "_keyed";
		ensureTopics(ohlcvDailyRawKeyedTopicName);
		
		// create dedup store
		final String dedupStoreName = getProcessorStoreTopicName(ohlcvDailyRawKeyedTopicName+"-dedup");
		StoreBuilder<KeyValueStore<SymbolTimestampKey,OHLCVRecord>> dedupStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(dedupStoreName),
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class),
						jsonSerdeRegistry.getSerde(OHLCVRecord.class));
		builder.addStateStore(dedupStoreBuilder);
		
		// build topology 
		sourceDataStreamsFacade.getOhlcvDailyRawStream()
		// map OHLCVDataRaw -> OHLCVData
		.map((key, rawRecord) -> {
			OHLCVRecord record = OHLCVRecord.from(rawRecord);
			return new KeyValue<>(record.getKey(), record);
		})
		// push to intermediate topic to repartition
		.through(ohlcvDailyRawKeyedTopicName, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(OHLCVRecord.class)))
		// deduplicate
		.transform(() -> new Transformer<SymbolTimestampKey, OHLCVRecord, KeyValue<SymbolTimestampKey, OHLCVRecord>> () {

			private KeyValueStore<SymbolTimestampKey,OHLCVRecord> state;
			
			@Override
			@SuppressWarnings("unchecked")
			public void init(ProcessorContext context) {
				this.state = (KeyValueStore<SymbolTimestampKey,OHLCVRecord>)context.getStateStore(dedupStoreName);
			}

			@Override
			public KeyValue<SymbolTimestampKey, OHLCVRecord> transform(SymbolTimestampKey key, OHLCVRecord value) {
				OHLCVRecord firstRecord = this.state.get(key);
				if(firstRecord == null) {
					this.state.put(key, value);
					return new KeyValue<>(key, value);
				}
				return null;
			}

			@Override
			public void close() {
				// nothing to do
			}
		}, dedupStoreName)
		// push to sink
		.to(SourceDataStreamsFacade.TOPIC_OHLCV_DAILY, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(OHLCVRecord.class)));
	}
}
