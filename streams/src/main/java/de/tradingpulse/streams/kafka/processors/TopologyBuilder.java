package de.tradingpulse.streams.kafka.processors;

import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;

/**
 * A builder to easily setup KStream topologies. Clients normally interact by
 * firstly initiating the TopologyBuilder, secondly assigning a KStream, and
 * then transforming the stream with any of the given methods:
 * <pre>
 *   StreamsBuilder streamsBuilder = ...
 *   KStream<...> sourceStream = ...
 *   
 *   TopologyBuilder
 *   .init(streamsBuilder)
 *   .from(
 *   	sourceStream,
 *      keySerde,
 *      valueSerde)
 *   ...
 *   to(topicName);
 * </pre>
 * 
 * TODO document the exact behavior
 * TODO add tests
 * TODO add developer documentation how to extend
 * TODO develop a plugin concept to dynamically enhance the functionality 
 *
 * @param <K> the key type of the initital stream
 * @param <V> the value type of the initial stream
 */
public class TopologyBuilder <K,V> {
	
	/**
	 * Initiates a new TopologyBuilder.<br>
	 * <br>
	 * Please note that the TopologyBuilder still needs to get a stream
	 * assigned by {@link #from(KStream, Serde, Serde)}. 
	 * 
	 * @param <K> the TopologyBuilder's key type
	 * @param <V> the TopologyBuilder's value type
	 * @param streamsBuilder a {@link StreamsBuilder} for internal use
	 * @return
	 * 	the initiated TopologyBuilder<K,V>
	 */
	public static <K,V> TopologyBuilder<K,V> init(
			StreamsBuilder streamsBuilder)
	{
		return new TopologyBuilder<>(streamsBuilder);
	}
	
	private final StreamsBuilder streamsBuilder;
	private KStream<K,V> stream;
	private Serde<K> keySerde;
	private Serde<V> valueSerde;
	
	private String topicsBaseName;
	
	private TopologyBuilder(
			StreamsBuilder streamsBuilder)
	{
		Objects.requireNonNull(streamsBuilder, "streamsBuilder");
		
		this.streamsBuilder = streamsBuilder;
	}
	
	private TopologyBuilder(
			StreamsBuilder streamsBuilder,
			KStream<K,V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde)
	{
		Objects.requireNonNull(streamsBuilder, "streamsBuilder");
		
		this.streamsBuilder = streamsBuilder;
		this.stream = stream;
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;
	}
	
	/**
	 * Returns the current stream.
	 * 
	 * @return 
	 * 	KStream<K,V>
	 */
	public KStream<K,V> getStream() {
		return this.stream;
	}
	
	/**
	 * Sets the topic base name to derive other topics names from.
	 * 
	 * @param topicsBaseName
	 * @return
	 * 	this TopologyBuilder<K,V>
	 */
	public TopologyBuilder<K,V> withTopicsBaseName(String topicsBaseName) {
		this.topicsBaseName = topicsBaseName;
		return this;
	}
	
	/**
	 * Sets the current stream and returns a new TopologyBuilder initiated
	 * with the current's TopologyBuilder's settings.
	 * 
	 * @param <NK> the stream's key type
	 * @param <NV> the stream's value type
	 * @param stream the new stream
	 * @param keySerde the key type {@link Serde}
	 * @param valueSerde the value type {@link Serde}
	 * @return
	 * 	a new initialized TopologyBuilder<NK,NV>
	 */
	public <NK,NV> TopologyBuilder<NK,NV> from(
			KStream<NK,NV> stream, 
			Serde<NK> keySerde, 
			Serde<NV> valueSerde)
	{
		Objects.requireNonNull(stream, "stream");
		Objects.requireNonNull(keySerde, "keySerde");
		Objects.requireNonNull(valueSerde, "valueSerde");
		
		return new TopologyBuilder<>(
				this.streamsBuilder, 
				stream, 
				keySerde, 
				valueSerde)
				.withTopicsBaseName(this.topicsBaseName);
	}
	
	/**
	 * Materializes the current stream into the given topic. The topic has
	 * to be created before.
	 * 
	 * @param topicName the target topic
	 * @return
	 * 	a new initialized TopologyBuilder<K,V> which stream is a stream
	 * 	reading from the topic
	 */
	public TopologyBuilder<K,V> through(String topicName) {
		Objects.requireNonNull(this.stream, "stream");
		Objects.requireNonNull(this.keySerde, "keySerde");
		Objects.requireNonNull(this.valueSerde, "valueSerde");
		Objects.requireNonNull(topicName, "topicName");
		
		KStream<K,V> topicBackedStream = this.stream
				.through(topicName, Produced.with(
						this.keySerde, 
						this.valueSerde));
		
		return new TopologyBuilder<>(
				this.streamsBuilder,
				topicBackedStream,
				this.keySerde,
				this.valueSerde)
				.withTopicsBaseName(topicName);
	}
	
	/**
	 * Materializes the current stream into the given topic. The topic has
	 * to be created before. <br>
	 * <br>
	 * This is a terminal operation.
	 * 
	 * @param topicName the target topic.
	 */
	public void to(String topicName) {
		Objects.requireNonNull(this.stream, "stream");
		Objects.requireNonNull(this.keySerde, "keySerde");
		Objects.requireNonNull(this.valueSerde, "valueSerde");
		Objects.requireNonNull(topicName, "topicName");
		
		this.stream
		.to(topicName, Produced.with(
				this.keySerde, 
				this.valueSerde));
	}
	
	/**
	 * Creates a new KStream from the current stream by changing the key.
	 *  
	 * @param <NK> the key type of the new stream
	 * @param rekeyFunction the function to specify the new key for each the original stream's records
	 * @param newKeySerde the new key {@link Serde}
	 * @return
	 * 	a new TopologyBuilder<NK,V> initiated with the new KStream
	 */
	public <NK> TopologyBuilder<NK,V> rekey(BiFunction<K,V, NK> rekeyFunction, Serde<NK> newKeySerde) {
		// TODO introduce RekeyBuilder
		// JoinBuilder, TransactionBuilder provide the pattern. This 
		// TopologyBuild should not know the details of how this manipulation
		// works (rekeyFunction!)
		
		Objects.requireNonNull(this.stream, "stream");
		Objects.requireNonNull(this.valueSerde, "valueSerde");
		Objects.requireNonNull(rekeyFunction, "rekeyFunction");
		Objects.requireNonNull(newKeySerde, "newKeySerde");
		
		KStream<NK,V> rekeyedStream = this.stream
		.map((key, value) -> new KeyValue<>(rekeyFunction.apply(key, value), value));
		
		return new TopologyBuilder<>(
				this.streamsBuilder,
				rekeyedStream,
				newKeySerde,
				this.valueSerde)
				.withTopicsBaseName(this.topicsBaseName);
	}
	
	/**
	 * Filters the current stream by applying the given predicate.
	 * 
	 * @param predicate the {@link Predicate} to filter the current stream
	 * @return
	 * 	a new initiated TopologyBuilder<K,V> with the filtered stream
	 */
	public TopologyBuilder<K,V> filter(Predicate<K, V> predicate) {
		// TODO introduce FilterBuilder
		// JoinBuilder, TransactionBuilder provide the pattern. This 
		// TopologyBuild should not know the details of how this manipulation
		// works (predicate!)
		
		Objects.requireNonNull(this.stream, "stream");
		Objects.requireNonNull(this.keySerde, "keySerde");
		Objects.requireNonNull(this.valueSerde, "valueSerde");
		Objects.requireNonNull(predicate, "predicate");
		
		return new TopologyBuilder<>(
				this.streamsBuilder,
				this.stream
					.filter(predicate),
				this.keySerde,
				this.valueSerde)
				.withTopicsBaseName(this.topicsBaseName);
	}
	
	/**
	 * De-duplicates the records of the current stream.<br>
	 *  
	 * @param <GK> the group key type (see {@link DedupBuilder#groupBy(BiFunction, Serde)})
	 * @param <DK> the group dedup value type (see {@link DedupBuilder#advanceBy(BiFunction)})
	 * 
	 * @return
	 * 	a new initiated {@link DedupBuilder} with the dedup'ed stream 
	 */
	public <GK,DV> DedupBuilder<K,V, GK,DV> dedup() {
		Objects.requireNonNull(this.stream, "stream");
		Objects.requireNonNull(this.keySerde, "keySerde");
		Objects.requireNonNull(this.valueSerde, "valueSerde");
		
		return new DedupBuilder<K,V, GK,DV> (
				this.streamsBuilder, 
				this.stream, 
				this.keySerde, 
				this.valueSerde)
				.withTopicsBaseName(this.topicsBaseName);
	}
	
	/**
	 * Creates a new KStream by (inner) joining the current stream (left) with
	 * the given other stream (right). <br>
	 * <br>
	 * The join will be backed by two changelog topics for each of the incoming
	 * records from the left and right stream. Clients have to specify the base
	 * name of these topics by calling {@link #withTopicsBaseName(String)} or
	 * {@link JoinBuilder#withTopicsBaseName(String)} at the returned
	 * JoinBuilder.<br>
	 * 
	 * @param <OV> the other (right) stream's value type
	 * @param <VR> the resulting stream's value type
	 * @param otherStream the other (right) stream
	 * @param otherValueSerde the other stream's value {@link Serde}
	 * @return
	 * 	a new initialized JoinBuilder<K,V, OV, VR>
	 */
	public <OV, VR> JoinBuilder<K,V, OV, VR> join(KStream<K,OV> otherStream, Serde<OV> otherValueSerde) {
		// TODO move parameters to JoinBuilder
		// TopologyBuild should not know the details of how this manipulation
		// works 
		
		Objects.requireNonNull(this.stream, "stream");
		Objects.requireNonNull(this.keySerde, "keySerde");
		Objects.requireNonNull(this.valueSerde, "valueSerde");
		Objects.requireNonNull(this.topicsBaseName, "topicsBaseName");
		Objects.requireNonNull(otherStream, "otherStream");
		Objects.requireNonNull(otherValueSerde, "otherValueSerde");
		
		return new JoinBuilder<K,V, OV, VR>(
				this.streamsBuilder,
				this.stream, 
				this.keySerde, 
				this.valueSerde, 
				otherStream, 
				otherValueSerde)
			.withTopicsBaseName(topicsBaseName);
	}


	/**
	 * Creates a new stream of TransactionRecords describing transactions found
	 * in this TopologyBuilder's stream.
	 * 
	 * @param <A> actually V as A
	 * @param <GK> the potential groupKey type, can be Void
	 * @return
	 * 	a new initialized TransactionBuilder
	 */
	@SuppressWarnings("unchecked")
	public <A extends AbstractIncrementalAggregateRecord, GK> TransactionBuilder<K,A, GK> transaction() {
		Objects.requireNonNull(this.stream, "stream");
		Objects.requireNonNull(this.keySerde, "keySerde");
		Objects.requireNonNull(this.valueSerde, "valueSerde");
		
		return (TransactionBuilder<K,A, GK>)new TransactionBuilder<>(
				this.streamsBuilder, 
				(KStream<K,A>)this.stream, 
				this.keySerde, 
				(Serde<A>)this.valueSerde)
				.withTopicsBaseName(this.topicsBaseName);
	}
	
	/**
	 * Creates a stream of transformed records.
	 * 
	 * @param <VR> the transformed record's type
	 * @return
	 * 	a new initialized TransformBuilder
	 */
	@SuppressWarnings("unchecked")
	public <VR> TransformBuilder<K,V, VR> transform() {
		Objects.requireNonNull(this.stream, "stream");
		Objects.requireNonNull(this.keySerde, "keySerde");
		Objects.requireNonNull(this.valueSerde, "valueSerde");
		
		return (TransformBuilder<K,V, VR>)new TransformBuilder<>(
				this.streamsBuilder, 
				this.stream, 
				this.keySerde, 
				this.valueSerde)
				.withTopicsBaseName(topicsBaseName);
	}
}