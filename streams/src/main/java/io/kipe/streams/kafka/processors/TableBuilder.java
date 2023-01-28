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

/**
 * TableBuilder is a Kafka Streams topology component that groups incoming data by a key.
 * <p>
 * The key is an empty string, so all data will be grouped into a single table.
 * <p>
 * The table is stored in a state store and can be accessed and modified through the transformer.
 *
 * Usage:
 *
 * Example:
 * <pre>
 *     {@code
 *     TODO
 *     }
 * </pre>
 *
 * @param <K> The key type of the input stream
 */
public class TableBuilder<K> extends AbstractTopologyPartBuilder<K, GenericRecord> {

	private static final String EMPTY = "";

	/**
	 * Construct a new TableBuilder.
	 *
	 * @param streamsBuilder The {@link StreamsBuilder} to use.
	 * @param stream         The {@link KStream} to transform.
	 * @param keySerde       The {@link Serde} to use for the key of the input stream.
	 * @param valueSerde     The {@link Serde} to use for the value of the input stream.
	 * @param topicsBaseName The base name of the topics for this builder.
	 */
	TableBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, GenericRecord> stream, 
			Serde<K> keySerde,
			Serde<GenericRecord> valueSerde, 
			String topicsBaseName) 
	{
		super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
	}

	/**
	 * Builds the topology component.
	 *
	 * @param resultKeySerde   The {@link Serde} to use for the key of the resulting stream.
	 * @param resultValueSerde The {@link Serde} to use for the value of the resulting stream.
	 * @return A {@link TopologyBuilder} for the resulting stream.
	 */
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

	/**
	 * {@link TableTransformer} is a class that implements the {@link Transformer} interface. It takes in a key and value of generic types and returns a {@link KeyValue} object containing a string key and a {@link TableRecord} object.
	 * <p>
	 * The {@link TableTransformer} class is used for grouping and storing records in a table like structure.
	 */
	static class TableTransformer<K> implements Transformer<K, GenericRecord, KeyValue<String, TableRecord<K,GenericRecord>>> {

		private final String stateStoreName;
		
		KeyValueStore<String, TableRecord<K,GenericRecord>> stateStore;

		/**
		 * Constructor for {@link TableTransformer}.
		 *
		 * @param stateStoreName the name of the state store to be used by this transformer.
		 */
		TableTransformer(String stateStoreName) {
			this.stateStoreName = stateStoreName;
		}

		/**
		 * Initializes the transformer by obtaining the state store from the {@link ProcessorContext}.
		 *
		 * @param context the {@link ProcessorContext} containing the state store.
		 */
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			this.stateStore = (KeyValueStore<String, TableRecord<K,GenericRecord>>)context.getStateStore(this.stateStoreName);
		}

		/**
		 * Transforms the input key and value by storing them in a table like structure represented by the {@link TableRecord} object.
		 *
		 * @param key   the key to be stored.
		 * @param value the value to be stored.
		 * @return a {@link KeyValue} object containing a string key and the updated {@link TableRecord} object.
		 */
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

		/**
		 * Closes the transformer. Currently, no action is performed in this method.
		 */
		@Override
		public void close() {
			// nothing to do
		}
		
	}
}
