package de.tradingpulse.streams.kafka.processors;

import static de.tradingpulse.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;

import io.micronaut.core.serialize.exceptions.SerializationException;

/**
 * Builds sequences of records and applies a function to the sequences. Each 
 * record starts a new sequence of the configured size.<br>
 * <br>
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {SOURCE[key:value]}
 *   
 *   <b>sequence</b>
 *     <b>groupBy</b>
 *       {FUNCTION[key,value]:groupKey}
 *     <b>size</b>
 *       {INTEGER:1}
 *     <b>as</b>
 *       {FUNCTION[value[]]:aggregate}
 *       
 *   to
 *     {TARGET[key:newValue]}
 * </pre> 
 *
 * @param <K>
 * @param <V>
 * @param <GK>
 * @param <VR>
 */
public class SequenceBuilder<K, V, GK, VR> extends AbstractTopologyPartBuilder<K, V, SequenceBuilder<K, V, GK, VR>>{

	private BiFunction<K,V, GK> groupKeyFunction;
	private Serde<GK> groupKeySerde;
	
	private int sequenceSize = 1;
	
	SequenceBuilder(
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
	public SequenceBuilder<K,V, GK, VR> groupBy(BiFunction<K,V, GK> groupKeyFunction, Serde<GK> groupKeySerde) {
		this.groupKeyFunction = groupKeyFunction;
		this.groupKeySerde = groupKeySerde;
		
		return this;
	}
	
	/**
	 * Configures the size of the sequences. The given argument must be
	 * greater than 0.
	 * 
	 * @param size the size of the sequences.
	 * 
	 * @return
	 * 	this builder
	 */
	public SequenceBuilder<K,V, GK, VR> size(int size) {
		if(size <= 0) {
			throw new IllegalArgumentException("size must be larger than 0");
		}
		
		this.sequenceSize = size;
		return this;
	}
	
	/**
	 * 
	 * @param aggregateFunction
	 * @param aggregateSerde
	 * @return
	 */
	public TopologyBuilder<K,VR> as(
			BiFunction<GK,List<V>, VR> aggregateFunction, 
			Class<V> valueClass,
			Serde<VR> resultValueSerde) 
	{
		Objects.requireNonNull(getTopicsBaseName(), "topicsBaseName");		
		Objects.requireNonNull(this.groupKeyFunction, "groupKeyFunction");
		Objects.requireNonNull(this.groupKeySerde, "groupKeySerde");
		Objects.requireNonNull(resultValueSerde, "resultValueSerde");
		
		final String stateStoreName = getProcessorStoreTopicName(getTopicsBaseName()+"-sequence");
		
		StoreBuilder<KeyValueStore<GK, List<List<V>>>> dedupStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
						this.groupKeySerde,
						new SequencesSerde<>(valueClass));
		this.streamsBuilder.addStateStore(dedupStoreBuilder);
		
		
		return createTopologyBuilder(
				this.stream
				.transform(
						() -> new SequenceTransformer<>(
								stateStoreName,
								this.groupKeyFunction,
								this.sequenceSize,
								aggregateFunction),
						stateStoreName), 
				this.keySerde, 
				resultValueSerde);
	}
	
	
	// ------------------------------------------------------------------------
	// SequenceTransformer
	// ------------------------------------------------------------------------
	
	static class SequenceTransformer <K,V, VR, GK>
	implements Transformer<K,V, KeyValue<K,VR>> 
	{
		private final String stateStoreName;
		private final BiFunction<K,V, GK> groupKeyFunction;
		private final int sequenceSize;
		private final BiFunction<GK,List<V>, VR> aggregateFunction;
		
		KeyValueStore<GK, List<List<V>>> stateStore;

		SequenceTransformer(
				String stateStoreName,
				BiFunction<K,V, GK> groupKeyFunction,
				int sequenceSize,
				BiFunction<GK,List<V>, VR> aggregateFunction)
		{
			this.stateStoreName = stateStoreName;
			this.groupKeyFunction = groupKeyFunction;
			this.sequenceSize = sequenceSize;
			this.aggregateFunction = aggregateFunction;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			this.stateStore = (KeyValueStore<GK, List<List<V>>>)context.getStateStore(stateStoreName);
		}

		@Override
		public KeyValue<K,VR> transform(K key, V value) {
			final GK groupKey = this.groupKeyFunction.apply(key, value);
			List<List<V>> groupSequences = this.stateStore.get(groupKey);
			
			if(groupSequences == null) {
				// we see that group for the very first time
				groupSequences = new LinkedList<>();
				
				// TODO: the group store grows indefinitly
				// outdated group keys aren't evicted, we might want to add
				// something like a ttl to evict incomplete sequences to free
				// up old keys
			}
			
			// add the value to all sequences
			groupSequences.forEach(sequence -> 
					sequence.add(value));

			// each value also starts a new sequence
			List<V> newSequence = new LinkedList<>();
			newSequence.add(value);
			groupSequences.add(newSequence);
			
			// aggregate the first sequence if it has all the records needed
			if(groupSequences.get(0).size() < this.sequenceSize) {
				this.stateStore.put(groupKey, groupSequences);
				return null;
			}
			
			List<V> completedSequence = groupSequences.remove(0);
			this.stateStore.put(groupKey, groupSequences);

			return new KeyValue<>(key, this.aggregateFunction.apply(groupKey, completedSequence));
		}

		@Override
		public void close() {
			// nothing to do
		}
		
	}
	
	static class SequencesSerde<T> implements Serializer<List<List<T>>>, Deserializer<List<List<T>>>, Serde<List<List<T>>> {

	    private final ObjectMapper mapper;
		private final CollectionType valueType;
		
		public SequencesSerde(Class<T> type) {
			this.mapper = new ObjectMapper();
			
			CollectionType innerCollectionType = mapper.getTypeFactory()
					.constructCollectionType(List.class, type);
			
			this.valueType = mapper.getTypeFactory()
					.constructCollectionType(List.class, innerCollectionType);
		}
		
		@Override
		public List<List<T>> deserialize(String topic, byte[] data) {
	        if (data == null) {
	            return null;
	        }
	        
	        try {
				return mapper.readValue(data, valueType);
			} catch (IOException e) {
				throw new SerializationException("Unable to deserialize data: " + data, e);
			}
		}

		@Override
		public byte[] serialize(String topic, List<List<T>> data) {
	        if (data == null) {
	            return null;
	        }
	        
	        try {
				return mapper.writeValueAsBytes(data);
			} catch (JsonProcessingException e) {
				throw new SerializationException("Unable to serialize data: " + data, e);			
			}
		}

	    @Override
	    public void configure(Map<String, ?> configs, boolean isKey) {
	        // no-op
	    }

		@Override
		public Serializer<List<List<T>>> serializer() {
			return this;
		}

		@Override
		public Deserializer<List<List<T>>> deserializer() {
			return this;
		}

		@Override
		public void close() {
			// no-op
		}
	}
	

}
