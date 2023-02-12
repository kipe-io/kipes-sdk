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
 * Builder to setup a (inner) join of two streams. Clients do not instantiate
 * this class directly but use {@link KipesBuilder#join(KStream, Serde)}.
 *
 * <p><b>Usage</b></p>
 * The JoinBuilder class provides a way to join two streams in Kafka Streams. It allows the clients
 * to set the window size and retention period for the join operation and specify a value joiner
 * function.
 *
 * <p><b>Example</b></p>
 * <pre>
 * KStream<String, Long> leftStream = ...;
 * KStream<String, Long> rightStream = ...;
 * Serde<String> keySerde = ...;
 * Serde<Long> valueSerde = ...;
 *
 * JoinBuilder<String, Long, Long, Long> joinBuilder = new JoinBuilder<>(
 *     streamsBuilder,
 *     leftStream,
 *     keySerde,
 *     valueSerde,
 *     rightStream,
 *     valueSerde,
 *     "topic-base-name");
 *
 * joinBuilder.withWindowSize(Duration.ofDays(1))
 *     .withRetentionPeriod(Duration.ofDays(7))
 *     .withValueJoiner((value1, value2) -> value1 + value2)
 *     .to("join-output-topic");
 * </pre>
 *
 * <p><b>Pseudo DSL</b></p>
 * <pre>
 *   from
 *     {STREAM[key:value]}
 *
 *   join
 *     {STREAM[key:otherValue]}
 *     windowSize|Before|After
 *       {DURATION}
 *     retentionPeriod
 *       {DURATION}
 *     as
 *       {FUNCTION(key,value,otherValue):joinValue}
 *
 *   to
 *     {TARGET[key:value]}
 * </pre>
 *
 * @param <K>  key type of both streams
 * @param <V>  value type of the left stream
 * @param <OV> value type of the right stream
 * @param <VR> value type of the joined stream
 */
public class JoinBuilder <K,V, OV, VR> extends AbstractTopologyPartBuilder<K, V>{
	
	private final KStream<K,OV> otherStream;
	private final Serde<OV> otherValueSerde;
	
	private Duration windowSizeBefore; 
	private Duration windowSizeAfter; 
	private Duration retentionPeriod;

	/**
	 * Constructor for the JoinBuilder class.
	 *
	 * @param streamsBuilder  StreamsBuilder instance for the Kafka Streams library
	 * @param stream          Left stream for the join operation
	 * @param keySerde        Serde for the key of the left stream
	 * @param valueSerde      Serde for the value of the left stream
	 * @param otherStream     Right stream for the join operation
	 * @param otherValueSerde Serde for the value of the right stream
	 * @param topicsBaseName  Base name for the topics used in the join operation
	 */
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

	/**
	 * Sets the window size for the join operation.
	 *
	 * @param windowSize The duration of the window size before and after the join event.
	 * @return The JoinBuilder with the updated window size.
	 */
	public JoinBuilder<K,V, OV, VR> withWindowSize(Duration windowSize) {
		this.windowSizeBefore = windowSize;
		this.windowSizeAfter = windowSize;
		return this;
	}

	/**
	 * Sets the window size before the join event.
	 *
	 * @param windowSizeBefore The duration of the window size before the join event.
	 * @return The JoinBuilder with the updated window size before.
	 */
	public JoinBuilder<K,V, OV, VR> withWindowSizeBefore(Duration windowSizeBefore) {
		this.windowSizeBefore = windowSizeBefore;
		return this;
	}

	/**
	 * Sets the window size after the join event.
	 *
	 * @param windowSizeAfter The duration of the window size after the join event.
	 * @return The JoinBuilder with the updated window size after.
	 */
	public JoinBuilder<K,V, OV, VR> withWindowSizeAfter(Duration windowSizeAfter) {
		this.windowSizeAfter = windowSizeAfter;
		return this;
	}

	/**
	 * Sets the retention period for the join operation.
	 *
	 * @param retentionPeriod The duration of the retention period for the join operation.
	 * @return The JoinBuilder with the updated retention period.
	 */
	public JoinBuilder<K,V, OV, VR> withRetentionPeriod(Duration retentionPeriod) {
		this.retentionPeriod = retentionPeriod;
		return this;
	}

	/**
	 * Assembles the joined stream using a named materialized changelog store.
	 * <p>
	 * This method performs a join operation on the current stream and the specified other stream, using the provided ValueJoiner and Serde for the result value. The join is performed within a specified window, defined by the before and after window sizes and a retention period.
	 * <p>
	 * Clients must specify the base name for the topics used in the join operation using the {@link #withTopicsBaseName(String)} method before calling this method.
	 *
	 * @param joiner           the {@link ValueJoiner} used to combine the values from the current and other streams
	 * @param resultValueSerde the {@link Serde} to be used for the result value
	 * @return a KipesBuilder with the joined stream
	 * @throws NullPointerException if any of the required parameters (topicsBaseName, joiner, resultValueSerde) are null
	 */
	public KipesBuilder<K,VR> as(ValueJoiner<V, OV, VR> joiner, Serde<VR> resultValueSerde) {
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
		
		return createKipesBuilder(
				joinedStream, 
				this.keySerde, 
				resultValueSerde);
	}
}