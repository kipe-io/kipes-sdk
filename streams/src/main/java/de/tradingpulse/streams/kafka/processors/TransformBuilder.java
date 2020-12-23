package de.tradingpulse.streams.kafka.processors;

import java.util.Arrays;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Builder to setup a stream transforming incoming records into zero or more 
 * outgoing records.<br>
 * <br>
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {SOURCE[key:value]}
 *   
 *   <b>transform</b>
 *     <b>into</b>
 *       {FUNCTION(key,value):newValue[]}
 *     <b>as</b>
 *       newValue
 *   
 *   to
 *     {TARGET[key:newValue]}
 * </pre>
 *
 * @param <K>
 * @param <V>
 * @param <VR>
 */
public class TransformBuilder<K,V, VR> extends AbstractTopologyPartBuilder<K, V, TransformBuilder<K,V, VR>>{

	private BiFunction<K,V, Iterable<VR>> transformFunction;
	
	TransformBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde)
	{
		super(streamsBuilder, stream, keySerde, valueSerde);
	}

	public TransformBuilder<K,V, VR> intoSingleRecord(BiFunction<K,V, VR> transformFunction) {
		
		this.transformFunction = new BiFunction<K, V, Iterable<VR>>() {
			@Override
			public Iterable<VR> apply(K key, V value) {
				return Arrays.asList(transformFunction.apply(key, value));
			}
		};
		
		return this;
	}
	
	public TransformBuilder<K,V, VR> intoMultiRecords(BiFunction<K,V, Iterable<VR>> transformFunction) {
		this.transformFunction = transformFunction;
		
		return this;
	}

	public TopologyBuilder<K,VR> as(Serde<VR> resultValueSerde) {
		return createTopologyBuilder(
				this.stream
				.flatMapValues(
						(key, value) ->
							this.transformFunction.apply(key,value)), 
				this.keySerde, 
				resultValueSerde);
	}
}
