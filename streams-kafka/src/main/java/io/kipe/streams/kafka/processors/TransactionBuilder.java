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
 * A builder to create a stream of transactions. Transactions are sequences of input records and can be defined using
 * two BiPredicates: one to start the transaction and another to end the transaction. Additionally, the records in the
 * transaction can be grouped using a groupByFunction.
 * <p>
 * The input records to the TransactionBuilder class must be a KStream. The key and value of this stream must be
 * serialized and deserialized using Serdes. The TransactionBuilder class creates a TransactionRecord which contains the
 * input value and the group key. The TransactionRecord is then emitted to a target stream.
 * <p>
 * Example:
 *
 * <pre>{@code
 * BiFunction<String, String, String> groupKeyFunction = (key, value) -> "group1";
 * Serde<String> groupKeySerde = Serdes.String();
 *
 * BiPredicate<String, String> startsWithPredicate = (key, value) -> value.startsWith("start");
 * BiPredicate<String, String> endsWithPredicate = (key, value) -> value.endsWith("end");
 *
 * TransactionBuilder<String, String, String> transactionBuilder = new TransactionBuilder<>(
 *     streamsBuilder,
 *     stream,
 *     Serdes.String(),
 *     Serdes.String(),
 *     "topic-base-name"
 * );
 *
 * transactionBuilder.groupBy(groupKeyFunction, groupKeySerde)
 *     .startsWith(startsWithPredicate)
 *     .endsWith(endsWithPredicate)
 *     .emitType(EmitType.ALL)
 *     .build();}</pre>
 * <p>
 * In this example, the groupKeyFunction takes in a String key and a String value and returns a String group key. The
 * groupKeySerde is used to serialize and deserialize the group key. The startsWithPredicate and endsWithPredicate are
 * used to determine when a transaction starts and ends respectively. In this example, a transaction starts when the
 * value starts with "start" and ends when the value ends with "end". The emitType is set to {@link EmitType#ALL}, which
 * means that all the records in the transaction will be emitted as TransactionRecords.
 *
 * @param <K>  the key type.
 * @param <V>  the input value type.
 * @param <GK> the groupKey type.
 */
public class TransactionBuilder<K,V, GK> 
extends AbstractTopologyPartBuilder<K, V>
{
	private static final Logger LOG = LoggerFactory.getLogger(TransactionBuilder.class);
	
	private BiFunction<K,V, GK> groupKeyFunction;
	private Serde<GK> groupKeySerde;
	
	private BiPredicate<K, V> startsWithPredicate;
	private BiPredicate<K, V> endsWithPredicate;
	
	private EmitType emitType = EmitType.ALL;

    /**
     * Creates an instance of the TransactionBuilder.
     *
     * @param streamsBuilder the StreamsBuilder instance
     * @param stream         the input KStream
     * @param keySerde       the key serde
     * @param valueSerde     the value serde
     * @param topicsBaseName the base name of the topics
     */
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
     * * <p>
     * It uses the provided groupKeySerde.
     *
     * @param groupKeyFunction the function to calculate the GroupKey.
     * @param groupKeySerde    the serde for the GroupKey.
     * @return this builder.
     */
	public TransactionBuilder<K,V, GK> groupBy(BiFunction<K,V, GK> groupKeyFunction, Serde<GK> groupKeySerde) {
		this.groupKeyFunction = groupKeyFunction;
		this.groupKeySerde = groupKeySerde;
		
		return this;
	}
	
	/**
	 * Configures a GroupKeyFunction to group incoming records.
	 * <p>
	 * It uses the default groupKeySerde.
	 *
	 * @param groupKeyFunction the function to calculate the GroupKey.
	 * @return this builder.
	 */
	public TransactionBuilder<K,V, GK> groupBy(BiFunction<K,V, GK> groupKeyFunction) {
		return groupBy(groupKeyFunction, null);
	}

    /**
     * Configures the {@link BiPredicate} function to identify records starting a transaction.
     *
     * @param startsWithPredicate the predicate used to determine if a record starts a transaction.
     * @return this builder.
     */
	public TransactionBuilder<K,V, GK> startsWith(BiPredicate<K, V> startsWithPredicate) {
		this.startsWithPredicate = startsWithPredicate;
		return this;
	}

	/**
	 * Configures the {@link BiPredicate} function to identify records ending a transaction.
	 *
	 * @param endsWithPredicate the predicate used to determine if a record ends a transaction.
	 * @return this builder.
	 */
	public TransactionBuilder<K,V, GK> endsWith(BiPredicate<K, V> endsWithPredicate) {
		this.endsWithPredicate = endsWithPredicate;
		return this;
	}

    /**
     * Configures which records get emitted in the final TransactionRecords. Defaults to {@link EmitType#ALL}.
     *
     * @param emitType the type specifying when to emit the transaction record.
     * @return this builder
     */
	public TransactionBuilder<K,V, GK> emit(EmitType emitType) {
		this.emitType = emitType;
		return this;
	}

	/**
	 * Assembles the transaction transformer and returns a new KipesBuilder configured with the resulting
	 * TransactionRecord stream.<br>
	 * <br>
	 * The processing is backed by a named materialized changelog store. Clients need to specify the base name with
	 * {@link KipesBuilder#withTopicsBaseName(String)} before.
	 * <p>
	 * If a non-null value is provided for the serdes parameter, it will be used as the serde for the resulting stream.
	 * Otherwise, the default resultValueSerde will be used.
	 *
	 * @return a new KipesBuilder
	 * @throws NullPointerException if `getTopicsBaseName()`, `groupKeyFunction`, `groupKeySerde`,
	 *                              `startsWithPredicate`, `endsWithPredicate`, or `emitType` is null.
	 */
	public KipesBuilder<K, TransactionRecord<GK, V>> as(Serde<TransactionRecord<GK, V>> resultValueSerde) {
		Objects.requireNonNull(getTopicsBaseName(), "topicsBaseName");		
		Objects.requireNonNull(this.groupKeyFunction, "groupKeyFunction");
		if (this.groupKeySerde == null) {
			LOG.warn("The default groupKeySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		Objects.requireNonNull(this.startsWithPredicate, "startsWithPredicate");
		Objects.requireNonNull(this.endsWithPredicate, "endsWithPredicate");
		Objects.requireNonNull(this.emitType, "emitType");
		
		
		final String stateStoreName = getProcessorStoreTopicName(getTopicsBaseName()+"-transaction");
		
		StoreBuilder<KeyValueStore<GK, TransactionRecord<GK, V>>> dedupStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
						this.groupKeySerde,
						resultValueSerde);
		this.streamsBuilder.addStateStore(dedupStoreBuilder);
		
		
		return createKipesBuilder(
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
	
	/**
	 * Assembles the transaction transformer and returns a new KipesBuilder configured with the resulting
	 * TransactionRecord stream.<br>
	 * <br>
	 * The processing is backed by a named materialized changelog store. Clients need to specify the base name with
	 * {@link KipesBuilder#withTopicsBaseName(String)} before.
	 * <p>
	 * The default resultValueSerde will be used.
	 *
	 * @return a new KipesBuilder
	 * @throws NullPointerException if `getTopicsBaseName()`, `groupKeyFunction`, `groupKeySerde`,
	 *                              `startsWithPredicate`, `endsWithPredicate`, or `emitType` is null.
	 */
	public KipesBuilder<K, TransactionRecord<GK, V>> as() {
		return as(null);
	}
	
	// ------------------------------------------------------------------------
	// EmitType
	// ------------------------------------------------------------------------

    /**
     * Enumeration to configure which records of a transaction will be emitted in a TransactionRecord.
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

        /**
         * Check if the provided EmitType is covered by this EmitType.
         *
         * @param emitType the type to check.
         * @return true if the provided EmitType is covered by this EmitType.
         */
		public boolean isCovered(EmitType emitType) {
			return this == emitType || covered.contains(emitType);
		}
	}
	
	// ------------------------------------------------------------------------
	// TransactionTransformer
	// ------------------------------------------------------------------------

    /**
     * A Transformer class that implements the functionality of grouping values based on a group key function and
     * detecting start and end of transactions.
     * <p>
     * Emits the transaction record based on the specified emit type.
     *
     * @param <K>  the key type of the input records.
     * @param <V>  the value type of the input records.
     * @param <GK> the group key type.
     */
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

        /**
         * Constructor for TransactionTransformer.
         *
         * @param stateStoreName      the state store name used to store the transaction records.
         * @param groupKeyFunction    the function used to extract the group key from the input record.
         * @param startsWithPredicate the predicate used to determine if a record starts a transaction.
         * @param endsWithPredicate   the predicate used to determine if a record ends a transaction.
         * @param emitType            the emit type specifying when to emit the transaction record.
         */
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

        /**
         * Initializes the transformer with the provided processor context.
         *
         * @param context The processor context.
         */
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			this.stateStore = (KeyValueStore<GK,TransactionRecord<GK, V>>)context.getStateStore(stateStoreName);
		}

        /**
         * Transforms a key-value pair by checking if the key-value pair starts or ends a transaction.
         * <p>
         * If the key-value pair starts a transaction, a new {@link TransactionRecord} is created and added to the state
         * store.
         * <p>
         * If the key-value pair is ongoing in a transaction, it is added to the corresponding
         * {@link TransactionRecord}.
         * <p>
         * If the key-value pair ends a transaction, the corresponding {@link TransactionRecord} is removed from the
         * state store and emitted.
         *
         * @param key   the key of the input key-value pair.
         * @param value the value of the input key-value pair.
         * @return a key-value pair where the key is the input key and the value is the corresponding
         * {@link TransactionRecord} if the transaction is ended, otherwise null.
         */
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

        /**
         * Determines if the current key-value pair starts a new transaction.
         *
         * @param key   The key of the current record.
         * @param value The value of the current record.
         * @return true if the current key-value pair starts a new transaction, false otherwise.
         */
		boolean startsWith(K key, V value) {
			return this.startsWithPredicate.test(key, value);
		}

        /**
         * Determines if the current key-value pair ends a transaction.
         *
         * @param key   The key of the current record.
         * @param value The value of the current record.
         * @return true if the current key-value pair ends a transaction, false otherwise.
         */
		boolean endsWith(K key, V value) {
			return this.endsWithPredicate.test(key, value);
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
