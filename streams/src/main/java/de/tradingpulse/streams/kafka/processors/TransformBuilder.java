package de.tradingpulse.streams.kafka.processors;

import java.util.Arrays;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class TransformBuilder<K,V, VR> extends AbstractTopologyPartBuilder<K, V, TransformBuilder<K,V, VR>>{

	private BiFunction<K,V, VR> transformFunction;
	
	TransformBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde)
	{
		super(streamsBuilder, stream, keySerde, valueSerde);
	}
	
	public TransformBuilder<K,V, VR> into(BiFunction<K,V, VR> transformFunction) {
		this.transformFunction = transformFunction;
		
		return this;
	}

	public TopologyBuilder<K,VR> as(Serde<VR> resultValueSerde) {
		return createTopologyBuilder(
				this.stream
				.flatMapValues(
						(key, value) ->
							Arrays.asList(this.transformFunction.apply(key,value))), 
				this.keySerde, 
				resultValueSerde);
	}
}
