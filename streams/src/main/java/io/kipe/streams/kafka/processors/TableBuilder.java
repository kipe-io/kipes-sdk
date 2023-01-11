package io.kipe.streams.kafka.processors;

import static io.kipe.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import io.kipe.streams.kafka.processors.recordtypes.TableRecord;
import io.kipe.streams.recordtypes.GenericRecord;

// TODO documentation
// TODO tests
public class TableBuilder<K> extends AbstractTopologyPartBuilder<K, GenericRecord> {

	private static final String EMPTY = "";

	TableBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, GenericRecord> stream, 
			Serde<K> keySerde,
			Serde<GenericRecord> valueSerde, 
			String topicsBaseName) 
	{
		super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
	}

	public TopologyBuilder<String,TableRecord<K,GenericRecord>> build(
			Serde<String> resultKeySerde,
			Serde<TableRecord<K,GenericRecord>> resultValueSerde)
	{
		Objects.requireNonNull(resultKeySerde, "resultKeySerde");
		Objects.requireNonNull(resultValueSerde, "resultValueSerde");
		Objects.requireNonNull(getTopicsBaseName(), "topicsBaseName must be set");
		
		final String stateStoreName = getProcessorStoreTopicName(getTopicsBaseName()+"-tablebuilder");
		
		StoreBuilder<KeyValueStore<String, TableRecord<K,GenericRecord>>> tableStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
						resultKeySerde,
						resultValueSerde);
		this.streamsBuilder.addStateStore(tableStoreBuilder);
		
		return createTopologyBuilder(
				stream.transform(
						() -> new TableTransformer<K>(
								stateStoreName), 
						stateStoreName), 
				resultKeySerde, 
				resultValueSerde);
	}
	
	// ------------------------------------------------------------------------
	// TableTransformer
	// ------------------------------------------------------------------------

	static class TableTransformer<K> implements Transformer<K, GenericRecord, KeyValue<String, TableRecord<K,GenericRecord>>> {

		private final String stateStoreName;
		
		KeyValueStore<String, TableRecord<K,GenericRecord>> stateStore;
		
		TableTransformer(String stateStoreName) {
			this.stateStoreName = stateStoreName;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			this.stateStore = (KeyValueStore<String, TableRecord<K,GenericRecord>>)context.getStateStore(this.stateStoreName);
		}

		@Override
		public KeyValue<String, TableRecord<K,GenericRecord>> transform(K key, GenericRecord value) {
			String groupKey = EMPTY;
			TableRecord<K,GenericRecord> table = this.stateStore.get(groupKey);
			
			if(table == null) {
				table = new TableRecord<>();
			}
			
			table.put(key, value);
			this.stateStore.put(groupKey, table);
			
			return new KeyValue<>(groupKey, table);
		}

		@Override
		public void close() {
			// nothing to do
		}
		
	}
}
