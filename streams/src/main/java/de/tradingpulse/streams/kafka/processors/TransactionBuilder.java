package de.tradingpulse.streams.kafka.processors;

import static de.tradingpulse.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

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

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import de.tradingpulse.streams.recordtypes.TransactionRecord;

/**
 * Builder to setup a stream of transactions as found in the input stream.<br>
 * <br>
 * A transaction is a sequence of input records starting and ending with 
 * records identified by {@link BiPredicate}s. You can further group records by
 * specifying a groupByFunction.<br>
 * <br>
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {SOURCE[key:value]}
 *   
 *   <b>transaction</b>
 *     <b>groupBy</b>
 *       {FUNCTION(key,value):groupKey}
 *     <b>startsWith</b>
 *       {FUNCTION(key,value):boolean}
 *     <b>endsWith</b>
 *       {FUNCTION(key,value):boolean}
 *     <b>as</b>
 *        TransactionRecord[value,groupKey]
 *   to
 *     {TARGET[key:TransactionRecord[value,groupKey]]}
 * </pre>
 * 
 * 
 * TODO: describe the exact behavior
 * TODO add tests
 * 
 * @param <K> the key type
 * @param <V> the input value type
 * @param <GK> the groupKey type
 */
public class TransactionBuilder<K,V extends AbstractIncrementalAggregateRecord, GK> 
extends AbstractTopologyPartBuilder<K, V, TransactionBuilder<K,V, GK>>
{
	private BiFunction<K,V, GK> groupKeyFunction;
	private Serde<GK> groupKeySerde;
	
	private BiPredicate<K, V> startsWithPredicate;
	private BiPredicate<K, V> endsWithPredicate;
	
	TransactionBuilder(
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
	public TransactionBuilder<K,V, GK> groupBy(BiFunction<K,V, GK> groupKeyFunction, Serde<GK> groupKeySerde) {
		this.groupKeyFunction = groupKeyFunction;
		this.groupKeySerde = groupKeySerde;
		
		return this;
	}
	
	/**
	 * Configures the {@link BiPredicate} function to identify records starting 
	 * a transaction.
	 * 
	 * @param startsWithPredicate
	 * 
	 * @return
	 * 	this builder
	 */
	public TransactionBuilder<K,V, GK> startsWith(BiPredicate<K, V> startsWithPredicate) {
		this.startsWithPredicate = startsWithPredicate;
		return this;
	}
	
	/**
	 * Configures the {@link BiPredicate} function to identify records ending a
	 * transaction.
	 * 
	 * @param endsWithPredicate
	 * 
	 * @return
	 * 	this builder
	 */
	public TransactionBuilder<K,V, GK> endsWith(BiPredicate<K, V> endsWithPredicate) {
		this.endsWithPredicate = endsWithPredicate;
		return this;
	}
	
	/**
	 * Assembles the transaction transformer and returns a new TopologyBuilder
	 * configured with the resulting TransactionRecord stream.<br>
	 * <br>
	 * The processing is backed by a named materialized changelog store. Clients
	 * need to specify the base name with 
	 * {@link #withTopicsBaseName(String)} before.
	 *  
	 * @return
	 * 	a new TopologyBuilder
	 */
	public TopologyBuilder<K, TransactionRecord<V, GK>> as(Serde<TransactionRecord<V, GK>> resultValueSerde) {
		Objects.requireNonNull(getTopicsBaseName(), "topicsBaseName");		
		Objects.requireNonNull(this.groupKeyFunction, "groupKeyFunction");
		Objects.requireNonNull(this.groupKeySerde, "groupKeySerde");
		Objects.requireNonNull(this.startsWithPredicate, "startsWithPredicate");
		Objects.requireNonNull(this.endsWithPredicate, "endsWithPredicate");
		
		final String stateStoreName = getProcessorStoreTopicName(getTopicsBaseName()+"-transaction");
		
		StoreBuilder<KeyValueStore<GK, TransactionRecord<V, GK>>> dedupStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
						this.groupKeySerde,
						resultValueSerde);
		this.streamsBuilder.addStateStore(dedupStoreBuilder);
		
		
		return createTopologyBuilder(
				this.stream
				.transform(
						() -> new TransactionTransformer<>(
								stateStoreName,
								this.groupKeyFunction,
								this.startsWithPredicate,
								this.endsWithPredicate),
						stateStoreName), 
				this.keySerde, 
				resultValueSerde);
	}
	
	// ------------------------------------------------------------------------
	// TransactionTransformer
	// ------------------------------------------------------------------------

	static class TransactionTransformer <K,V extends AbstractIncrementalAggregateRecord, GK> 
	implements Transformer<K,V, KeyValue<K, TransactionRecord<V, GK>>>
	{
		private static final Logger LOG = LoggerFactory.getLogger(TransactionTransformer.class);
		
		private final String stateStoreName;
		private final BiFunction<K,V, GK> groupKeyFunction;
		private final BiPredicate<K, V> startsWithPredicate;
		private final BiPredicate<K, V> endsWithPredicate;
		
		private KeyValueStore<GK,TransactionRecord<V, GK>> stateStore;
		
		TransactionTransformer(
				String stateStoreName, 
				BiFunction<K,V, GK> groupKeyFunction,
				BiPredicate<K, V> startsWithPredicate, 
				BiPredicate<K, V> endsWithPredicate)
		{
			this.stateStoreName = stateStoreName;
			this.groupKeyFunction = groupKeyFunction;
			this.startsWithPredicate = startsWithPredicate;
			this.endsWithPredicate = endsWithPredicate;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			this.stateStore = (KeyValueStore<GK,TransactionRecord<V, GK>>)context.getStateStore(stateStoreName);
		}

		@Override
		public KeyValue<K, TransactionRecord<V, GK>> transform(K key, V value) {
			final GK groupKey = this.groupKeyFunction.apply(key, value);
			TransactionRecord<V, GK> transactionRecord = this.stateStore.get(groupKey);
			
			// store empty? yea, startswith? no, ignore
			if(transactionRecord == null && startsWith(key, value)) {
				// store empty? yea, startswith? yea, create TransactionRecord add value
				transactionRecord = TransactionRecord.createFrom(value);
				transactionRecord.setGroupKey(groupKey);
				
				LOG.debug("transaction.startsWith groupKey:{} value:{}", groupKey, value);
			}
			
			if(transactionRecord == null) {
				// store empty? yea, startswith? no, ignore
				return null;
			}
			
			transactionRecord.addUnique(value);
			this.stateStore.put(groupKey, transactionRecord);
			
			if(!endsWith(key, value)) {
				LOG.debug("transaction.continued groupKey:{} value:{}", groupKey, value);
				return null;
			}
			
			// endswith? yea, delete TransactionRecord, emit TransactionRecord
			this.stateStore.delete(groupKey);
			
			LOG.debug("transaction.endsWith groupKey:{} value:{}", groupKey, value);
			
			return new KeyValue<>(key, transactionRecord);
		}

		boolean startsWith(K key, V value) {
			return this.startsWithPredicate.test(key, value);
		}

		boolean endsWith(K key, V value) {
			return this.endsWithPredicate.test(key, value);
		}
		
		@Override
		public void close() {
			// nothing to do
		}
	}
}
