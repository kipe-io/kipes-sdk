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

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import de.tradingpulse.streams.kafka.factories.JsonSerdeFactory;
import de.tradingpulse.streams.recordtypes.TransactionRecord;

/**
 * Builder to setup a stream of transactions as found in the input stream.<br>
 * <br>
 * A transaction is a sequence of input records grouped by a same key and 
 * starting and ending with records identified by {@link BiPredicate}s.  
 * 
 * TODO: describe the exact behavior
 * 
 * @param <K> the key type
 * @param <V> the input value type
 */
public class TransactionBuilder<K,V extends AbstractIncrementalAggregateRecord> 
extends AbstractTopologyPartBuilder<K, V, TransactionBuilder<K,V>>
{
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
	 * Configures the {@link BiPredicate} function to identify records starting 
	 * a transaction.
	 * 
	 * @param startsWithPredicate
	 * 
	 * @return
	 * 	this builder
	 */
	public TransactionBuilder<K,V> startsWith(BiPredicate<K, V> startsWithPredicate) {
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
	public TransactionBuilder<K,V> endsWith(BiPredicate<K, V> endsWithPredicate) {
		this.endsWithPredicate = endsWithPredicate;
		return this;
	}
	
	/**
	 * Configures a GroupKeyFunction to group incoming records. If there is no
	 * GroupKeyFunction configured (null) then all incoming records are member
	 * of a single group.
	 * 
	 * @param <GK> the GroupKey's type
	 * @param groupKeyFunction the function to calculate the GroupKey
	 * @param groupKeySerde the serde for the GroupKey
	 * @return
	 * 	this builder
	 */
	public <GK> TransactionBuilder<K,V> groupBy(BiFunction<K,V, GK> groupKeyFunction, Serde<GK> groupKeySerde) {
		return null;
	}
	
	/**
	 * Assembles the transaction transformer and returns a new TopologyBuilder
	 * configured with the resulting TransactionRecord stream.
	 *  
	 * @return
	 * 	a new TopologyBuilder
	 */
	public TopologyBuilder<K, TransactionRecord<V>> collect() {
		Objects.requireNonNull(getTopicsBaseName(), "topicsBaseName");		
		Objects.requireNonNull(this.startsWithPredicate, "startsWithPredicate");
		Objects.requireNonNull(this.endsWithPredicate, "endsWithPredicate");
		
		final String stateStoreName = getProcessorStoreTopicName(getTopicsBaseName()+"-transaction");
		
		@SuppressWarnings("unchecked")
		final Serde<TransactionRecord<V>> serde = JsonSerdeFactory.getJsonSerde(
				(Class<TransactionRecord<V>>)(Class<?>)TransactionRecord.class);
		
		StoreBuilder<KeyValueStore<K, TransactionRecord<V>>> dedupStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
						this.keySerde,
						serde);
		this.streamsBuilder.addStateStore(dedupStoreBuilder);
		
		
		return createTopologyBuilder(
				this.stream
				.transform(
						() -> new TransactionTransformer<>(
								stateStoreName, 
								this.startsWithPredicate,
								this.endsWithPredicate),
						stateStoreName), 
				this.keySerde, 
				serde);
	}
	
	// ------------------------------------------------------------------------
	// TransactionTransformer
	// ------------------------------------------------------------------------

	static class TransactionTransformer <K,V extends AbstractIncrementalAggregateRecord> 
	implements Transformer<K,V, KeyValue<K, TransactionRecord<V>>>
	{

		private final String stateStoreName;
		private final BiPredicate<K, V> startsWithPredicate;
		private final BiPredicate<K, V> endsWithPredicate;
		
		private KeyValueStore<K,TransactionRecord<V>> stateStore;
		
		TransactionTransformer(
				String stateStoreName, 
				BiPredicate<K, V> startsWithPredicate, 
				BiPredicate<K, V> endsWithPredicate)
		{
			this.stateStoreName = stateStoreName;
			this.startsWithPredicate = startsWithPredicate;
			this.endsWithPredicate = endsWithPredicate;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			this.stateStore = (KeyValueStore<K,TransactionRecord<V>>)context.getStateStore(stateStoreName);
		}

		@Override
		public KeyValue<K, TransactionRecord<V>> transform(K key, V value) {
			
			TransactionRecord<V> transactionRecord = this.stateStore.get(key);
			
			// store empty? yea, startswith? no, ignore
			if(transactionRecord == null && startsWith(key, value)) {
				// store empty? yea, startswith? yea, create TransactionRecord add value
				transactionRecord = TransactionRecord.createFrom(value);
			}
			
			if(transactionRecord == null) {
				// store empty? yea, startswith? no, ignore
				return null;
			}
			
			transactionRecord.addUnique(value);
			this.stateStore.put(key, transactionRecord);
			
			if(!endsWith(key, value)) {
				return null;
			}
			
			// endswith? yea, delete TransactionRecord, emit TransactionRecord
			this.stateStore.delete(key);
			
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
