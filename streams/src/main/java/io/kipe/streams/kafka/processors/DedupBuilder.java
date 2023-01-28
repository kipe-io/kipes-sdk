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
 * Builder to setup a stream to de-duplicate incoming records. De-duplication
 * can be configured on two levels.
 *
 * First, clients must specify how incoming records are to be grouped
 * (see {@link #groupBy(BiFunction, Serde)}). Incoming records are generally
 * de-duplicated in these groups.
 *
 * Second, client can specify how the stream of records within these groups is
 * de-duplicated (see {@link #advanceBy(BiFunction)}).
 * <p>
 * <br>
 * <b>Example Usage:</b>
 * <pre>
 * {@code
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
 *     .advanceBy((key, value) -> value);
 *
 *   KStream<String, String> dedupedStream = dedupBuilder.build();
 * }
 * </pre>
 *
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {SOURCE[key:value]}
 *
 *   <b>dedup</b>
 *     <b>groupBy</b>
 *       {FUNCTION(key,value):groupKey}
 *     <b>advanceBy</b>
 *       {FUNCTION(key,value):groupDedupValue}
 *     <b>emitFirst</b>
 *
 *   to
 *     {TARGET[key:value]}
 * </pre>
 * 
 * @param <K> the incoming and outgoing streams' key type
 * @param <V> the incoming and outgoing streams' value type
 * @param <GK> the group key's type (see {@link #groupBy(BiFunction, Serde)})
 * @param <DV> the dedupValue's type (see {@link #advanceBy(BiFunction)})
 */
public class DedupBuilder<K,V, GK,DV> extends AbstractTopologyPartBuilder<K, V> {

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
	 *
	 * @param groupKeyFunction the function to calculate the GroupKey
	 * @param groupKeySerde the serde for the GroupKey
	 * @return
	 * 	this builder
	 */
	public DedupBuilder<K,V, GK,DV> groupBy(BiFunction<K,V, GK> groupKeyFunction, Serde<GK> groupKeySerde) {
		this.groupKeyFunction = groupKeyFunction;
		this.groupKeySerde = groupKeySerde;

		return this;
	}

	/**
	 * Configures a value de-duplication function to identify equal records
	 * within a group of records (a dedup value group). <br>
	 * <br>
	 * If two records generate the same dedupValue then those records are 
	 * semantically the same and one of those gets ignored. <br>
	 * <br>
	 * Furthermore, if the dedupValue changes within the stream of incoming
	 * records (exactly, within the stream of a single group) then a new
	 * dedup value group is started. One record of each dedup value group is
	 * emitted.<br>
	 * <br>
	 * If the dedupValueFunction is not set then the incoming records get
	 * de-duplicated after the {@link #groupBy(BiFunction, Serde)} function
	 * only.
	 * 
	 * @param dedupValueFunction the function to calculate the dedupValue
	 * @return
	 * 	this builder
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
     * {@link #withTopicsBaseName(String)}. Additionally, the group key function and group key serde
     * must also be set using {@link #groupBy(KeyValueMapper)} and {@link #withGroupKeySerde(Serde)}, respectively.
     *
     * @return A new TopologyBuilder initialized with the deduplication stream.
     */
	public TopologyBuilder<K,V> emitFirst() {
		Objects.requireNonNull(getTopicsBaseName(), "topicsBaseName must be set");
		Objects.requireNonNull(this.groupKeyFunction, "groupBy groupKeyFunction must be set");
		Objects.requireNonNull(this.groupKeySerde, "groupBy groupKeySerde must be set");
		
		final String stateStoreName = getProcessorStoreTopicName(getTopicsBaseName()+"-dedup");
		
		StoreBuilder<KeyValueStore<GK,V>> dedupStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
						this.groupKeySerde,
						this.valueSerde);
		this.streamsBuilder.addStateStore(dedupStoreBuilder);
		
		return createTopologyBuilder(
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
	 * The DedupTransformer class is a stateful implementation of the Transformer interface.
	 * It can be used to filter out duplicate records in a Kafka stream based on the group key and dedup value.
	 * <p>
	 * This transformer uses a KeyValueStore to maintain state of the last seen value for each group key.
	 * The init() method is used to initialize the state store and can be called multiple times if the transformer is used in a repartitioning context.
	 * <p>
	 * The transform() method takes in a key and value pair, applies the provided group key function and dedup value function to extract the group key and dedup value respectively. If a value for the same group key is found in the state store, it compares the dedup value of the stored value with the current value. If they are equal, the record is considered a duplicate and is not emitted, otherwise the current value is emitted and stored as the last seen value for that group key.
	 * <p>
	 * If no dedup value function is provided, only the group key is used for deduplication.
	 * <p>
	 * The close() method is used to release any resources held by the transformer and is called when the transformer is closed.
	 *
	 * @param <K>  The type of the key.
	 * @param <V>  The type of the value.
	 * @param <GK> The type of the group key.
	 * @param <DV> The type of the dedup value.
	 *
	 */

	/**
	 * DedupTransformer is a Kafka Streams {@link Transformer} that deduplicates incoming records based on a
	 * user-defined grouping key and, optionally, a user-defined value to compare for equality. The
	 * transformer uses a state store to keep track of the last seen value for each group key, and only
	 * emits new records if the group key or the comparison value has changed.
	 *
	 * @param <K>  the key type of the incoming records
	 * @param <V>  the value type of the incoming records
	 * @param <GK> the type of the grouping key used for deduplication
	 * @param <DV> the type of the comparison value used for deduplication
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
		 * @param stateStoreName     the name of the state store used to keep track of the last seen values for each group key
		 * @param groupKeyFunction   a user-defined function that takes in a record key and value and returns the group key used for deduplication
		 * @param groupDedupFunction a user-defined function that takes in a record key and value and returns the comparison value used for deduplication.
		 *                           If this is set to null, only the group key will be used for deduplication
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


//		/**
//		 * Initializes the transformer by getting the state store from the {@link ProcessorContext}
//		 *
//		 * @param context the {@link ProcessorContext} for the current Kafka Streams application
//		 */

		/**
		 * The `init` method is called by the Kafka Streams library when the topology is initialized.
		 * In this method, we are getting the state store that was previously created and named in the topology.
		 * This state store will be used to store the previously seen values for deduplication.
		 *
		 * @param context the ProcessorContext provided by the Kafka Streams library.
		 */
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			this.stateStore = (KeyValueStore<GK,V>)context.getStateStore(stateStoreName);
		}

		/**
		 * The main method of the transformer, called for each input key-value pair.
		 * It uses the provided groupKeyFunction and dedupValueFunction to extract the group key and dedup value respectively.
		 * Then it checks the state store for the presence of the group key, and if present, it compares the dedup value of the previous value with the current value.
		 * If the dedup value of the previous value is equal to the current value, it means that the current value is a duplicate and it is ignored.
		 * Otherwise, the current value is considered unique and is emitted.
		 * The state store is always updated with the latest value for the group key.
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
