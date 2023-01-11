package io.kipe.streams.kafka.processors;

import static io.kipe.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;
import static io.kipe.streams.kafka.processors.TransactionBuilder.EmitType.END;
import static io.kipe.streams.kafka.processors.TransactionBuilder.EmitType.ONGOING;
import static io.kipe.streams.kafka.processors.TransactionBuilder.EmitType.START;
import static io.kipe.streams.kafka.processors.TransactionBuilder.EmitType.START_AND_END;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

import io.kipe.streams.recordtypes.TransactionRecord;

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
 *     <b>emit</b>
 *       ALL
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
public class TransactionBuilder<K,V, GK> 
extends AbstractTopologyPartBuilder<K, V>
{
	private BiFunction<K,V, GK> groupKeyFunction;
	private Serde<GK> groupKeySerde;
	
	private BiPredicate<K, V> startsWithPredicate;
	private BiPredicate<K, V> endsWithPredicate;
	
	private EmitType emitType = EmitType.ALL;
	
	TransactionBuilder(
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
	 * Configures which records get emitted in the final TransactionRecords.
	 * Defaults to {@link EmitType#ALL}.
	 * 
	 * @param emitType
	 * 
	 * @return
	 * 	this builder
	 */
	public TransactionBuilder<K,V, GK> emit(EmitType emitType) {
		this.emitType = emitType;
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
	public TopologyBuilder<K, TransactionRecord<GK, V>> as(Serde<TransactionRecord<GK, V>> resultValueSerde) {
		Objects.requireNonNull(getTopicsBaseName(), "topicsBaseName");		
		Objects.requireNonNull(this.groupKeyFunction, "groupKeyFunction");
		Objects.requireNonNull(this.groupKeySerde, "groupKeySerde");
		Objects.requireNonNull(this.startsWithPredicate, "startsWithPredicate");
		Objects.requireNonNull(this.endsWithPredicate, "endsWithPredicate");
		Objects.requireNonNull(this.emitType, "emitType");
		
		
		final String stateStoreName = getProcessorStoreTopicName(getTopicsBaseName()+"-transaction");
		
		StoreBuilder<KeyValueStore<GK, TransactionRecord<GK, V>>> dedupStoreBuilder =
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
								this.endsWithPredicate,
								this.emitType),
						stateStoreName), 
				this.keySerde, 
				resultValueSerde);
	}
	
	// ------------------------------------------------------------------------
	// EmitType
	// ------------------------------------------------------------------------

	/**
	 * Enumeration to configure which records of a transaction will be emitted
	 * in a TransactionRecord. 
	 */
	public enum EmitType {
		/** records starting a transaction will be emitted */ 
		START,
		/** records within a transaction will be emitted, but not those starting or ending the transaction */ 
		ONGOING,
		/** records ending a transaction will be emitted */ 
		END,
		/** all records of a transaction will be emitted */ 
		ALL(START, ONGOING, END),
		/** START and END */
		START_AND_END(START, END);
		
		private List<EmitType> covered;
		
		private EmitType() {
			this.covered = Collections.emptyList(); 
		}
		
		private EmitType(EmitType...coveredTypes ) {
			this.covered = Arrays.asList(coveredTypes);
		}
		
		public boolean isCovered(EmitType emitType) {
			return this == emitType || covered.contains(emitType);
		}
	}
	
	// ------------------------------------------------------------------------
	// TransactionTransformer
	// ------------------------------------------------------------------------

	static class TransactionTransformer <K,V, GK> 
	implements Transformer<K,V, KeyValue<K, TransactionRecord<GK, V>>>
	{
		private static final Logger LOG = LoggerFactory.getLogger(TransactionTransformer.class);
		
		private final String stateStoreName;
		private final BiFunction<K,V, GK> groupKeyFunction;
		private final BiPredicate<K, V> startsWithPredicate;
		private final BiPredicate<K, V> endsWithPredicate;
		private final EmitType emitType;
		
		KeyValueStore<GK,TransactionRecord<GK, V>> stateStore;
		
		TransactionTransformer(
				String stateStoreName, 
				BiFunction<K,V, GK> groupKeyFunction,
				BiPredicate<K, V> startsWithPredicate, 
				BiPredicate<K, V> endsWithPredicate,
				EmitType emitType)
		{
			this.stateStoreName = stateStoreName;
			this.groupKeyFunction = groupKeyFunction;
			this.startsWithPredicate = startsWithPredicate;
			this.endsWithPredicate = endsWithPredicate;
			this.emitType = emitType;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			this.stateStore = (KeyValueStore<GK,TransactionRecord<GK, V>>)context.getStateStore(stateStoreName);
		}

		@Override
		public KeyValue<K, TransactionRecord<GK, V>> transform(K key, V value) {
			final GK groupKey = this.groupKeyFunction.apply(key, value);
			TransactionRecord<GK, V> transactionRecord = this.stateStore.get(groupKey);
			
			EmitType currentTXNType = null;

			if(transactionRecord == null && startsWith(key, value)) {
				// store empty? yea, startswith? yea, create TransactionRecord add value
				transactionRecord = new TransactionRecord<GK, V>();
				transactionRecord.setGroupKey(groupKey);
				
				currentTXNType = START;
				
				LOG.trace("transaction.startsWith groupKey:{} value:{}", groupKey, value);
			}
			
			if(transactionRecord == null) {
				// store empty? yea, startswith? no, ignore
				return null;
			}
			
			if(!endsWith(key, value)) {
				if(currentTXNType != START) {
					currentTXNType = ONGOING;
					LOG.trace("transaction.continued groupKey:{} value:{}", groupKey, value);
				}
				
			} else {
				currentTXNType = currentTXNType == START? START_AND_END : END;
				
				LOG.trace("transaction.endsWith groupKey:{} value:{}", groupKey, value);
			}
			
			if(	this.emitType.isCovered(currentTXNType)
				|| (currentTXNType == START_AND_END 
					&& (this.emitType.isCovered(START) || this.emitType.isCovered(END))))
			{
				// emit record if current record txn type is covered
				transactionRecord.addUnique(value);
				
				LOG.trace("transaction.emit.{}.covers groupKey:{} value:{} currentTXNType:{}", this.emitType, groupKey, value, currentTXNType);
			}
			
			if(!currentTXNType.isCovered(END)) {
				// endswith?, no
				this.stateStore.put(groupKey, transactionRecord);
				
				return null;
			}
			
			// endswith? yea, delete TransactionRecord, emit TransactionRecord
			this.stateStore.delete(groupKey);
			
			LOG.debug("transaction.emit.{} groupKey:{} key:{} transaction:{} ", this.emitType, groupKey, key, transactionRecord);
			
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
