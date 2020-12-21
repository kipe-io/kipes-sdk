package de.tradingpulse.streams.kafka.processors;

import java.time.Duration;
import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.Stores;

/**
 * Builder to setup a (inner) join of two streams. Clients do not instanciate
 * this class directly but use {@link TopologyBuilder#join(KStream, Serde)}.
 * 
 * TODO document the exact behavior
 * TODO add tests
 *
 * @param <K> key type of both streams
 * @param <V> value type of the left stream
 * @param <OV> value type of the right stream
 * @param <VR> value type of the joined stream
 */
public class JoinBuilder <K,V, OV, VR> extends AbstractTopologyPartBuilder<K, V, JoinBuilder <K,V, OV, VR>>{
	
	private final KStream<K,OV> otherStream;
	private final Serde<OV> otherValueSerde;
	
	private Duration windowSizeAfter; 
	private Duration retentionPeriod;
	
	JoinBuilder(
			StreamsBuilder streamsBuilder,
			KStream<K, V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde, 
			KStream<K, OV> otherStream,
			Serde<OV> otherValueSerde)
	{
		super(streamsBuilder, stream, keySerde, valueSerde);
		
		Objects.requireNonNull(otherStream, "otherStream");
		Objects.requireNonNull(otherValueSerde, "otherValueSerde");
		
		this.otherStream = otherStream;
		this.otherValueSerde = otherValueSerde;
	}
	
	public JoinBuilder<K,V, OV, VR> withWindowSizeAfter(Duration windowAfterSize) {
		this.windowSizeAfter = windowAfterSize;
		return this;
	}
	
	public JoinBuilder<K,V, OV, VR> withRetentionPeriod(Duration retentionPeriod) {
		this.retentionPeriod = retentionPeriod;
		return this;
	}
	
	public TopologyBuilder<K,VR> as(ValueJoiner<V, OV, VR> joiner, Serde<VR> resultValueSerde) {
		Objects.requireNonNull(getTopicsBaseName(), "topicsBaseName");
		Objects.requireNonNull(this.windowSizeAfter, "windowSizeAfter");
		Objects.requireNonNull(this.retentionPeriod, "retentionPeriod");
		Objects.requireNonNull(joiner, "joiner");
		Objects.requireNonNull(resultValueSerde, "resultValueSerde");
		
		KStream<K,VR> joinedStream = 
				this.stream
				.join(
						this.otherStream,
						joiner,
						JoinWindows
						.of(Duration.ZERO)
						.after(this.windowSizeAfter)
						.grace(this.retentionPeriod),
						StreamJoined.<K,V,OV>with(
								Stores.persistentWindowStore(
										getTopicsBaseName()+"-join-store-left", 
										this.retentionPeriod.plus(this.windowSizeAfter), 
										this.windowSizeAfter, 
										true), 
								Stores.persistentWindowStore(
										getTopicsBaseName()+"-join-store-right", 
										this.retentionPeriod.plus(this.windowSizeAfter), 
										this.windowSizeAfter, 
										true))
						.withKeySerde(this.keySerde)
						.withValueSerde(this.valueSerde)
						.withOtherValueSerde(this.otherValueSerde));
		
		return createTopologyBuilder(
				joinedStream, 
				this.keySerde, 
				resultValueSerde);
	}
}