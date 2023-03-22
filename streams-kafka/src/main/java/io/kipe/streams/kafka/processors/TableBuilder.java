/*
 * Kipes SDK for Kafka - The High-Level Event Processing SDK.
 * Copyright © 2023 kipe.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
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
 * A builder for constructing table component that groups incoming data by a key. Clients do not instantiate this class
 * directly but use {@link KipesBuilder#table()}.
 * <p>
 * The key is an empty string, so all data will be grouped into a single table.
 * <p>
 * The table is stored in a state store and can be accessed and modified through the transformer.
 * <p>
 * Example:
 * <pre>{@code
 * KStream<String, String> stream = streamsBuilder.stream("input-topic");
 * Serde<String> keySerde = Serdes.String();
 * Serde<String> valueSerde = Serdes.String();
 *
 * TableBuilder<String> tableBuilder = new TableBuilder<>(
 *         builder,
 *         stream,
 *         keySerde,
 *         genericRecordSerde,
 *         "topicsBaseName"
 * );
 *
 * tableBuilder.build();}</pre>
 * <p>
 * In this example, a Kafka Stream with a String key and value is created and the key and value serialization and
 * deserialization is defined using the keySerde and valueSerde objects. The TableBuilder is then instantiated with the
 * streamsBuilder, stream, keySerde and valueSerde as arguments, and the build method is called to create the topology
 * component that groups incoming data by key and stores it in a state store accessible through the transformer.
 *
 * @param <K> The key type of the input stream.
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
	 * @return A {@link KipesBuilder} for the resulting stream.
	 */
	public KipesBuilder<String,TableRecord<K,GenericRecord>> build(
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
		
		return createKipesBuilder(
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
	 * {@link TableTransformer} is a class that implements the {@link Transformer} interface. It takes in a key and
	 * value of generic types and returns a {@link KeyValue} object containing a string key and a {@link TableRecord}
	 * object.
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
		 * Transforms the input key and value by storing them in a table like structure represented by the
		 * {@link TableRecord} object.
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
		 * Closes the transformer. Currently, no action is performed in this
		 * method.
		 */
		@Override
		public void close() {
			// nothing to do
		}
		
	}
}
