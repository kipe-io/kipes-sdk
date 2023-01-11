package io.kipe.streams.kafka.processors;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Builder to setup a stream transforming incoming records into zero or more 
 * outgoing records with transformed keys, values, or both.<br>
 * <br>
 * Clients select the intended transformation by starting with one of the
 * {@code intoXXX(...)} methods followed and finalized  by the related
 * {@code asXXX(...)} method. <br>
 * <br>
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {SOURCE[key:value]}
 *   
 *   <b>transform</b>
 *     <b>change|new</b>
 *       {FUNCTION(key,value):{newKey,newValue}[]}
 *     <b>as</b>
 *       {newKey,newValue}
 *   
 *   to
 *     {TARGET[newKey:newValue]}
 * </pre>
 * 
 * @param <K> the source stream's key type
 * @param <V> the source stream's value type
 * @param <KR> the target stream's key type
 * @param <VR> the target stream's value type
 */
// TODO introduce sub builders to improve encapsulation of the individual cases
// It's currently to easy to combine wrong intoXXX and asXXX variants.

// TODO introduce asXXX variants to simplify same key/value type transformations
// Transform not only supports changing types but also changing quantities. In
// latter cases it would be nice to have asXXX methods which do not ask for new
// serdes. 
// NOTE: makes sense only if we got sub builders (see to do above)

// TODO add tests
public class TransformBuilder<K,V, KR,VR> extends AbstractTopologyPartBuilder<K, V>{

	private BiFunction<K,V, Iterable<VR>> transformValueFunction;
	private BiFunction<K,V, Iterable<KR>> transformKeyFunction;
	private BiFunction<K,V, Iterable<KeyValue<KR,VR>>> transformKeyValueFunction;
	
	TransformBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde,
			String topicsBaseName)
	{
		super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
	}

	// ------------------------------------------------------------------------
	// transform values
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public TransformBuilder<K,V, K,VR> changeValue(BiFunction<K,V, VR> transformValueFunction) {
		
		this.transformValueFunction = (key, value) -> {
			VR result = transformValueFunction.apply(key, value);
			if(result == null) {
				return Collections.emptyList();
			} else {
				return Arrays.asList(transformValueFunction.apply(key, value));
			}
		};
		
		return (TransformBuilder<K,V, K,VR>)this;
	}
	
	@SuppressWarnings("unchecked")
	public TransformBuilder<K,V, K,VR> newValues(BiFunction<K,V, Iterable<VR>> transformValueFunction) {
		this.transformValueFunction = transformValueFunction;
		
		return (TransformBuilder<K,V, K,VR>)this;
	}

	public TopologyBuilder<K,VR> asValueType(Serde<VR> resultValueSerde) {
		Objects.requireNonNull(this.transformValueFunction, "transformValueFunction");
		
		return createTopologyBuilder(
				this.stream
				.flatMapValues(
						(key, value) ->
							this.transformValueFunction.apply(key,value)), 
				this.keySerde, 
				resultValueSerde);
	}
	
	// ------------------------------------------------------------------------
	// transform keys
	// ------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	public TransformBuilder<K,V, KR,V> changeKey(BiFunction<K,V, KR> transformKeyFunction) {
		this.transformKeyFunction = (key, value) -> Arrays.asList(transformKeyFunction.apply(key, value));
		
		return (TransformBuilder<K,V, KR,V>)this;
	}
	
	@SuppressWarnings("unchecked")
	public TransformBuilder<K,V, KR,V> newKeys(BiFunction<K,V, Iterable<KR>> transformKeyFunction) {
		this.transformKeyFunction = transformKeyFunction;
		
		return (TransformBuilder<K,V, KR,V>)this;
	}

	public TopologyBuilder<KR,V> asKeyType(Serde<KR> resultKeySerde) {
		Objects.requireNonNull(this.transformKeyFunction, "transformKeyFunction");
		return createTopologyBuilder(
				this.stream
				.flatMap(
						(key, value) -> {
							List<KeyValue<KR,V>> keyValues = new LinkedList<>();
							
							this.transformKeyFunction.apply(key,value)
							.forEach(resultKey -> keyValues.add(new KeyValue<>(resultKey, value)));
							
							return keyValues;
						}), 
				resultKeySerde, 
				this.valueSerde);
	}
	
	// ------------------------------------------------------------------------
	// transform keys and values
	// ------------------------------------------------------------------------
	
	public TransformBuilder<K,V, KR,VR> newKeyValues(BiFunction<K,V, Iterable<KeyValue<KR,VR>>> transformKeyValueFunction) {
		this.transformKeyValueFunction = transformKeyValueFunction;
		
		return this;
	}
	
	public TopologyBuilder<KR,VR> asKeyValueType(Serde<KR> resultKeySerde, Serde<VR> resultValueSerde) {
		Objects.requireNonNull(this.transformKeyValueFunction, "transformKeyValueFunction");
		return createTopologyBuilder(
				this.stream
				.flatMap(
						(key, value) -> 
							this.transformKeyValueFunction.apply(key,value)), 
				resultKeySerde, 
				resultValueSerde);
		
	}

}
