package de.tradingpulse.streams.kafka.processors;

import static de.tradingpulse.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;

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
 * can be configured on two levels. <br>
 * <br>
 * First, clients must specify how incoming records are to be grouped 
 * (see {@link #groupBy(BiFunction, Serde)}). Incoming records are generally 
 * de-duplicated in these groups.<br>
 * <br>
 * Second, client can specify how the stream of records within these groups is
 * de-duplicated (see {@link #advanceBy(BiFunction)}).
 *  
 * @param <K> the incoming and outgoing streams' key type
 * @param <V> the incoming and outgoing streams' value type
 * @param <GK> the group key's type (see {@link #groupBy(BiFunction, Serde)})
 * @param <DV> the dedupValue's type (see {@link #advanceBy(BiFunction)})
 */
public class DedupBuilder<K,V, GK,DV> extends AbstractTopologyPartBuilder<K, V, DedupBuilder<K,V, GK, DV>> {

	private BiFunction<K,V, GK> groupKeyFunction;
	private Serde<GK> groupKeySerde;

	private BiFunction<K,V, DV> dedupValueFunction;
	
	DedupBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde) 
	{
		super(streamsBuilder, stream, keySerde, valueSerde);
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
	 * Assembles a dedup stream which emits the first value for the specified
	 * configuration.<br>
	 * <br>
	 * The processing is backed by a named materialized changelog store. Clients
	 * need to specify the base name with 
	 * {@link #withTopicsBaseName(String)} before.
	 *   
	 * @return
	 * 	a new TopologyBuilder initialized with the dedup stream
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
	
	static class DedupTransformer<K,V, GK, DV> implements Transformer<K,V, KeyValue<K,V>> {

		private static final Logger LOG = LoggerFactory.getLogger(DedupTransformer.class);
		
		private final String stateStoreName;
		private final BiFunction<K, V, GK> groupKeyFunction;
		private final BiFunction<K, V, DV> dedupValueFunction;
		
		KeyValueStore<GK,V> stateStore;
		
		DedupTransformer(
				String stateStoreName, 
				BiFunction<K, V, GK> groupKeyFunction,
				BiFunction<K, V, DV> groupDedupFunction)
		{
			this.stateStoreName = stateStoreName;
			this.groupKeyFunction = groupKeyFunction;
			this.dedupValueFunction = groupDedupFunction;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			this.stateStore = (KeyValueStore<GK,V>)context.getStateStore(stateStoreName);
		}

		@Override
		public KeyValue<K,V> transform(K key, V value) {
			final GK groupKey = this.groupKeyFunction.apply(key, value);

			V storedValue = this.stateStore.get(groupKey);
			
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
