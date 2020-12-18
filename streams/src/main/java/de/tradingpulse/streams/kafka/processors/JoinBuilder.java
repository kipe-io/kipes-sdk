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
public class JoinBuilder <K,V, OV, VR> {
	
	private final StreamsBuilder streamsBuilder;
	private final KStream<K,V> stream;
	private final Serde<K> keySerde;
	private final Serde<V> valueSerde;
	private final KStream<K,OV> otherStream;
	private final Serde<OV> otherValueSerde;
	
	private String topicsBaseName;
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
		Objects.requireNonNull(streamsBuilder, "streamsBuilder");
		Objects.requireNonNull(stream, "stream");
		Objects.requireNonNull(keySerde, "keySerde");
		Objects.requireNonNull(valueSerde, "valueSerde");
		Objects.requireNonNull(otherStream, "otherStream");
		Objects.requireNonNull(otherValueSerde, "otherValueSerde");
		
		this.streamsBuilder = streamsBuilder;
		this.stream = stream;
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;
		this.otherStream = otherStream;
		this.otherValueSerde = otherValueSerde;
	}
	
	public JoinBuilder<K,V, OV,VR> withTopicsBaseName(String topicsBaseName) {
		this.topicsBaseName = topicsBaseName;
		return this;
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
		Objects.requireNonNull(this.topicsBaseName, "storeTopicsBaseName");
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
										this.topicsBaseName+"-join-store-left", 
										this.retentionPeriod.plus(this.windowSizeAfter), 
										this.windowSizeAfter, 
										true), 
								Stores.persistentWindowStore(
										this.topicsBaseName+"-join-store-right", 
										this.retentionPeriod.plus(this.windowSizeAfter), 
										this.windowSizeAfter, 
										true))
						.withKeySerde(this.keySerde)
						.withValueSerde(this.valueSerde)
						.withOtherValueSerde(this.otherValueSerde));
		
		return TopologyBuilder.init(this.streamsBuilder)
				.from(
						joinedStream, 
						this.keySerde, 
						resultValueSerde)
				.withTopicsBaseName(this.topicsBaseName);

	}
}