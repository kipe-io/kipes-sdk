/*
 * Kipe Streams Kafka - Kipe Streams SDK
 * Copyright © 2023 Kipe.io
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
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Builder to that is used to de-duplicate incoming records. Clients do not instantiate this class directly but use
 * {@link KipesBuilder#stats()}
 * <p>
 * The de-duplication process can be configured on two levels:
 * <ol>
 *     <li>Clients must specify how incoming records are to be grouped (see {@link #groupBy(BiFunction, Serde)}). Incoming records are generally de-duplicated in these groups.</li>
 *     <li>Client can specify how the stream of records within these groups is de-duplicated (see {@link #advanceBy(BiFunction)}).</li>
 * </ol>
 * <b>Example:</b>
 * <pre>{@code
 *   DedupBuilder<String,String,String,String> dedupBuilder = new DedupBuilder<>(
 *      streamsBuilder,
 *      stream,
 *      Serdes.String(),
 *      Serdes.String(),
 *      "dedup-topic"
 *   );
 *
 *   dedupBuilder
 *     .groupBy((key, value) -> value, Serdes.String())
 *     .advanceBy((key, value) -> value)
 *     .build();
 * }</pre>
 * <p>
 * In this example, we create a DedupBuilder and pass in a StreamsBuilder, a KStream of strings, and two Serdes of type
 * String. The dedup-topic parameter is the base name for the topics used by the DedupBuilder. Next, we group the
 * incoming records by their values and configure the deduplication function to use the record values to determine the
 * deduplication group. Finally, we call the build method to set up the stream and start the de-duplication process.
 *
 * @param <K>  the incoming and outgoing streams' key type
 * @param <V>  the incoming and outgoing streams' value type
 * @param <GK> the group key's type (see {@link #groupBy(BiFunction, Serde)})
 * @param <DV> the dedupValue's type (see {@link #advanceBy(BiFunction)})
 */
public class DedupBuilder<K,V, GK,DV> extends AbstractTopologyPartBuilder<K, V> {
	
	static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DedupBuilder.class);
	
	private BiFunction<K,V, GK> groupKeyFunction;
	private Serde<GK> groupKeySerde;

	private BiFunction<K,V, DV> dedupValueFunction;

	DedupBuilder(
			StreamsBuilder streamsBuilder,
			KStream<K, V> stream,
			Serde<K> keySerde,
			Serde<V> valueSerde,
			String topicsBaseName)
	{
		super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
	}

	/**
	 * Configures a GroupKeyFunction to group incoming records.
	 * <p>
	 * If a non-null value is provided for the serdes parameters, it will be used as the serde for the resulting key.
	 * Otherwise, the default serdes will be used.
	 *
	 * @param groupKeyFunction the function to calculate the GroupKey
	 * @param groupKeySerde    the serde for the GroupKey
	 * @return this builder
	 */
	public DedupBuilder<K,V, GK,DV> groupBy(BiFunction<K,V, GK> groupKeyFunction, Serde<GK> groupKeySerde) {
		this.groupKeyFunction = groupKeyFunction;
		this.groupKeySerde = groupKeySerde;

		return this;
	}
	
	/**
	 * Configures a GroupKeyFunction to group incoming records.
	 * <p>
	 * It uses the default serdes for the key.
	 *
	 * @param groupKeyFunction the function to calculate the GroupKey
	 * @return this builder
	 */
	public DedupBuilder<K,V, GK,DV> groupBy(BiFunction<K,V, GK> groupKeyFunction) {
		return groupBy(groupKeyFunction, null);
	}
	
	/**
	 * Configures a value de-duplication function to identify equal records within a group of records (a dedup value
	 * group). <br>
	 * <br>
	 * If two records generate the same dedupValue then those records are semantically the same and one of those gets
	 * ignored. <br>
	 * <br>
	 * Furthermore, if the dedupValue changes within the stream of incoming records (exactly, within the stream of a
	 * single group) then a new dedup value group is started. One record of each dedup value group is emitted.<br>
	 * <br>
	 * If the dedupValueFunction is not set then the incoming records get de-duplicated after the
	 * {@link #groupBy(BiFunction, Serde)} function only.
	 *
	 * @param dedupValueFunction the function to calculate the dedupValue
	 * @return this builder
	 */
	public DedupBuilder<K,V, GK, DV> advanceBy(BiFunction<K,V, DV> dedupValueFunction) {
		this.dedupValueFunction = dedupValueFunction;
		
		return this;
	}

	/**
	 * Assembles a deduplication stream which emits the first value for the specified configuration.
	 * <p>
	 * The stream uses a named materialized changelog store to keep track of the unique values.
	 * <p>
	 * Before calling this method, the client must set the base name for the topics using
	 * {@link KipesBuilder#withTopicsBaseName(String)}. Additionally, the group key function and group key serde must
	 * also be set using {@link {@link DedupBuilder#groupBy(BiFunction, Serde)}}.
	 *
	 * @return A new KipesBuilder initialized with the deduplication stream.
	 */
	public KipesBuilder<K,V> emitFirst() {
		Objects.requireNonNull(getTopicsBaseName(), "topicsBaseName must be set");
		Objects.requireNonNull(this.groupKeyFunction, "groupBy groupKeyFunction must be set");
		if (this.groupKeySerde == null) {
			LOG.warn("The default groupKeySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		
		final String stateStoreName = getProcessorStoreTopicName(getTopicsBaseName()+"-dedup");
		
		StoreBuilder<KeyValueStore<GK,V>> dedupStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
						this.groupKeySerde,
						this.valueSerde);
		this.streamsBuilder.addStateStore(dedupStoreBuilder);
		
		return createKipesBuilder(
				this.stream
				.transform(
						() -> new DedupTransformer<>(
								stateStoreName,
								this.groupKeyFunction,
								this.dedupValueFunction),
						stateStoreName),
				this.keySerde,
				this.valueSerde);

	}

	// ------------------------------------------------------------------------
	// DedupTransformer
	// ------------------------------------------------------------------------

	/**
	 * DedupTransformer is a Kafka Streams {@link Transformer} that deduplicates incoming records based on a
	 * user-defined grouping key and, optionally, a user-defined value to compare for equality. The transformer uses a
	 * state store to keep track of the last seen value for each group key, and only emits new records if the group key
	 * or the comparison value has changed.
	 *
	 * @param <K>  the key type of the incoming records.
	 * @param <V>  the value type of the incoming records.
	 * @param <GK> the type of the grouping key used for deduplication.
	 * @param <DV> the type of the comparison value used for deduplication.
	 */
	static class DedupTransformer<K,V, GK, DV> implements Transformer<K,V, KeyValue<K,V>> {

		private static final Logger LOG = LoggerFactory.getLogger(DedupTransformer.class);

		private final String stateStoreName;
		private final BiFunction<K, V, GK> groupKeyFunction;
		private final BiFunction<K, V, DV> dedupValueFunction;

		KeyValueStore<GK,V> stateStore;

		/**
		 * Creates a new instance of DedupTransformer
		 *
		 * @param stateStoreName     the name of the state store used to keep track of the last seen values for each
		 *                           group key
		 * @param groupKeyFunction   a user-defined function that takes in a record key and value and returns the group
		 *                           key used for deduplication
		 * @param groupDedupFunction a user-defined function that takes in a record key and value and returns the
		 *                           comparison value used for deduplication. If this is set to null, only the group key
		 *                           will be used for deduplication
		 */
		DedupTransformer(
				String stateStoreName,
				BiFunction<K, V, GK> groupKeyFunction,
				BiFunction<K, V, DV> groupDedupFunction)
		{
			this.stateStoreName = stateStoreName;
			this.groupKeyFunction = groupKeyFunction;
			this.dedupValueFunction = groupDedupFunction;
		}

		/**
		 * The is called by the Kafka Streams library when the topology is initialized. We are getting the state store
		 * that was previously created and named in the topology. This state store will be used to store the previously
		 * seen values for deduplication.
		 *
		 * @param context the ProcessorContext provided by the Kafka Streams library.
		 */
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			this.stateStore = (KeyValueStore<GK,V>)context.getStateStore(stateStoreName);
		}

		/**
		 * The main method of the transformer, called for each input key-value pair. It uses the provided
		 * groupKeyFunction and dedupValueFunction to extract the group key and dedup value respectively. Then it checks
		 * the state store for the presence of the group key, and if present, it compares the dedup value of the
		 * previous value with the current value. If the dedup value of the previous value is equal to the current
		 * value, it means that the current value is a duplicate, and it is ignored. Otherwise, the current value is
		 * considered unique and is emitted. The state store is always updated with the latest value for the group key.
		 *
		 * @param key   the input key
		 * @param value the input value
		 * @return the KeyValue pair to emit or null if the input value is a duplicate
		 */
		@Override
		public KeyValue<K,V> transform(K key, V value) {
			final GK groupKey = this.groupKeyFunction.apply(key, value);

			V storedValue = this.stateStore.get(groupKey);


			// The transformer uses the state store to check if the current record is a duplicate.
			// If the state store contains the current group key, the stored value is retrieved.
			// If the stored value is null, it means that this is the first time the group key is seen,
			// so the current record is emitted and its value is stored in the state store.


			// always keep last seen value
			this.stateStore.put(groupKey, value);

			if(storedValue == null) {
				// always emit first value
				LOG.debug("dedup.emitFirst.newGroupKey groupKey:{} key:{} value:{}",
						groupKey, key, value);

				return new KeyValue<>(key, value);
			}

			if(this.dedupValueFunction == null) {
				// dedup via groupKey, no further subset
				LOG.debug("dedup.duplicateIgnored.groupKey groupKey:{} key:{} value:{}",
						groupKey, key, value);

				return null;
			}

			// If the dedupValueFunction was provided, it is used to extract a subset value from the input record.
			// This subset value is then compared to the subset value of the stored value.
			// If they are equal, the current record is considered a duplicate and is not emitted.
			// If they are not equal, the current record is emitted and its value is stored in the state store.

			DV lastGroupDedupValue = this.dedupValueFunction.apply(key, storedValue);
			DV currentGroupDedupValue = this.dedupValueFunction.apply(key, value);

			if(lastGroupDedupValue.equals(currentGroupDedupValue)) {
				// no change, deduping
				LOG.debug("dedup.duplicateIgnored.groupDedupValue groupKey:{} groupDedupValue:{} key:{} value:{}",
						groupKey, currentGroupDedupValue, key, value);

				return null;
			}

			// new groupDedupValue, always emit first value
			LOG.debug("dedup.emitFirst.newGroupDedupValue groupKey:{} groupDedupValue:{} key:{} value:{}",
					groupKey, currentGroupDedupValue, key, value);

			return new KeyValue<>(key, value);
		}

		@Override
		public void close() {
			// nothing to do
		}
	}

}
