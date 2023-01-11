package io.kipe.streams.kafka.processors;

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
 * <br>
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {STREAM[key:value]}
 *   
 *   <b>join</b>
 *     {STREAM[key:otherValue]}
 *     <b>windowSize|Before|After</b>
 *       {DURATION}
 *     <b>retentionPeriod</b>
 *       {DURATION}
 *     <b>as</b>
 *       {FUNCTION(key,value,otherValue):joinValue}
 *   
 *   to
 *     {TARGET[key:value]}
 * </pre>
 * 
 * TODO document the exact behavior
 * TODO add tests
 *
 * @param <K> key type of both streams
 * @param <V> value type of the left stream
 * @param <OV> value type of the right stream
 * @param <VR> value type of the joined stream
 */
public class JoinBuilder <K,V, OV, VR> extends AbstractTopologyPartBuilder<K, V>{
	
	private final KStream<K,OV> otherStream;
	private final Serde<OV> otherValueSerde;
	
	private Duration windowSizeBefore; 
	private Duration windowSizeAfter; 
	private Duration retentionPeriod;
	
	JoinBuilder(
			StreamsBuilder streamsBuilder,
			KStream<K, V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde, 
			KStream<K, OV> otherStream,
			Serde<OV> otherValueSerde,
			String topicsBaseName)
	{
		super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
		
		Objects.requireNonNull(otherStream, "otherStream");
		Objects.requireNonNull(otherValueSerde, "otherValueSerde");
		
		this.otherStream = otherStream;
		this.otherValueSerde = otherValueSerde;
	}
	
	public JoinBuilder<K,V, OV, VR> withWindowSize(Duration windowSize) {
		this.windowSizeBefore = windowSize;
		this.windowSizeAfter = windowSize;
		return this;
	}
	
	public JoinBuilder<K,V, OV, VR> withWindowSizeBefore(Duration windowSizeBefore) {
		this.windowSizeBefore = windowSizeBefore;
		return this;
	}
	
	public JoinBuilder<K,V, OV, VR> withWindowSizeAfter(Duration windowSizeAfter) {
		this.windowSizeAfter = windowSizeAfter;
		return this;
	}
	
	public JoinBuilder<K,V, OV, VR> withRetentionPeriod(Duration retentionPeriod) {
		this.retentionPeriod = retentionPeriod;
		return this;
	}
	
	/**
	 * Assembles the joined stream.<br>
	 * <br>
	 * The processing is backed by a named materialized changelog store. Clients
	 * need to specify the base name with 
	 * {@link #withTopicsBaseName(String)} before.
	 * 
	 * @param joiner
	 * @param resultValueSerde
	 * @return
	 */
	public TopologyBuilder<K,VR> as(ValueJoiner<V, OV, VR> joiner, Serde<VR> resultValueSerde) {
		Objects.requireNonNull(getTopicsBaseName(), "topicsBaseName");
		
		if(this.windowSizeBefore == null) {
			this.windowSizeBefore = Duration.ZERO;
		}
		if(this.windowSizeAfter == null) {
			this.windowSizeAfter = Duration.ZERO;
		}
		
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
						.before(this.windowSizeBefore)
						.after(this.windowSizeAfter)
						.grace(this.retentionPeriod),
						StreamJoined.<K,V,OV>with(
								Stores.persistentWindowStore(
										getTopicsBaseName()+"-join-store-left", 
										this.retentionPeriod.plus(this.windowSizeBefore).plus(this.windowSizeAfter), 
										this.windowSizeBefore.plus(this.windowSizeAfter), 
										true), 
								Stores.persistentWindowStore(
										getTopicsBaseName()+"-join-store-right", 
										this.retentionPeriod.plus(this.windowSizeBefore).plus(this.windowSizeAfter), 
										this.windowSizeBefore.plus(this.windowSizeAfter), 
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