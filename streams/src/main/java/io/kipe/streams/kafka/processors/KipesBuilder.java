package io.kipe.streams.kafka.processors;

import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.slf4j.LoggerFactory;

import io.kipe.streams.recordtypes.GenericRecord;

/**
 * A builder to easily setup KStream topologies. Clients normally interact by
 * firstly initiating the KipesBuilder, secondly assigning a KStream, and
 * then transforming the stream with any of the given methods:
 * <pre>{@code
 *   StreamsBuilder streamsBuilder = ...
 *   KStream<...> sourceStream = ...
 *
 *   KipesBuilder
 *   .init(streamsBuilder)
 *   .from(
 *   	sourceStream,
 *      keySerde,
 *      valueSerde)
 *   ...
 *   to(topicName);}</pre>
 * <p>
 * TODO document the exact behavior
 * TODO add tests
 * TODO add developer documentation how to extend
 * TODO develop a plugin concept to dynamically enhance the functionality
 *
 * @param <K> the key type of the initital stream
 * @param <V> the value type of the initial stream
 */
public class KipesBuilder<K,V> {

	/**
	 * Instantiates a new instance of KipesBuilder.<br>
	 * <br>
	 * It's important to note that the {@link StreamsBuilder} must be assigned to the KipesBuilder
	 * using the {@link #from(KStream, Serde, Serde)} method before it can be used.
	 *
	 * @param <K>            the type of the key in the KipesBuilder
	 * @param <V>            the type of the value in the KipesBuilder
	 * @param streamsBuilder a required instance of {@link StreamsBuilder} for internal use
	 * @return an instance of {@link KipesBuilder}
	 */
	public static <K,V> KipesBuilder<K,V> init(
			StreamsBuilder streamsBuilder)
	{
		return new KipesBuilder<>(streamsBuilder);
	}
	
	static final org.slf4j.Logger LOG = LoggerFactory.getLogger(KipesBuilder.class);
	
	private final StreamsBuilder streamsBuilder;
	private KStream<K,V> stream;
	private Serde<K> keySerde;
	private Serde<V> valueSerde;
	private String topicsBaseName;
	
	private KipesBuilder(
			StreamsBuilder streamsBuilder)
	{
		Objects.requireNonNull(streamsBuilder, "streamsBuilder");
		
		this.streamsBuilder = streamsBuilder;
	}
	
	private KipesBuilder(
			StreamsBuilder streamsBuilder,
			KStream<K,V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde,
			String topicsBaseName)
	{
		Objects.requireNonNull(streamsBuilder, "streamsBuilder");
		
		this.streamsBuilder = streamsBuilder;
		this.stream = stream;
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;
		this.topicsBaseName = topicsBaseName;
	}

	/**
	 * Returns the current stream.
	 *
	 * @return the current {@link KStream}.
	 */
	public KStream<K,V> getStream() {
		return this.stream;
	}

	/**
	 * Sets the topic base name to derive other topics names from.
	 *
	 * @param topicsBaseName the base name for all topics.
	 * @return this {@link KipesBuilder}.
	 */
	public KipesBuilder<K,V> withTopicsBaseName(String topicsBaseName) {
		this.topicsBaseName = topicsBaseName;
		return this;
	}
	
	/**
	 * Sets the current stream and returns a new KipesBuilder initiated with the current's KipesBuilder's
	 * settings.
	 * <p>
	 * If a non-null value is provided for the serdes parameters, it will be used as the serde for the resulting stream.
	 * Otherwise, the default serdes will be used.
	 *
	 * @param <NK>       the stream's key type.
	 * @param <NV>       the stream's value type.
	 * @param stream     the new stream.
	 * @param keySerde   the key type {@link Serde}.
	 * @param valueSerde the value type {@link Serde}.
	 * @return a new initialized {@link KipesBuilder}.
	 */
	public <NK,NV> KipesBuilder<NK,NV> from(
			KStream<NK,NV> stream, 
			Serde<NK> keySerde, 
			Serde<NV> valueSerde)
	{
		Objects.requireNonNull(stream, "stream");
		if (keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		
		return new KipesBuilder<>(
				this.streamsBuilder, 
				stream, 
				keySerde, 
				valueSerde,
				this.topicsBaseName);
	}
	
	/**
	 * Sets the current stream and returns a new KipesBuilder initiated with the current's KipesBuilder's
	 * settings.
	 * <p>
	 * The default serdes will be used..
	 *
	 * @param <NK>   the stream's key type.
	 * @param <NV>   the stream's value type.
	 * @param stream the new stream.
	 * @return a new initialized {@link KipesBuilder}.
	 */
	public <NK, NV> KipesBuilder<NK, NV> from(KStream<NK, NV> stream) {
		return from(stream, null, null);
	}

	/**
	 * Logs each passing record as debug. Logger is the value class.
	 *
	 * @param identifier an identifier to include in the log message.
	 * @return a new instance of {@link KipesBuilder} with the logging added to the stream.
	 */
	public KipesBuilder<K,V> logDebug(String identifier) {
		return new KipesBuilder<>(
				this.streamsBuilder, 
				this.stream.map(
						(key, value) -> {
							LoggerFactory.getLogger(value.getClass())
							.debug("{} key: {} value: {}", identifier, key, value);
							
							return new KeyValue<>(key, value);
						}), 
				this.keySerde, 
				this.valueSerde,
				this.topicsBaseName);
	}

	/**
	 * Materializes the current stream into the given topic. The topic has to be created before.
	 *
	 * @param topicName the target topic.
	 * @return a new instance of {@link KipesBuilder} with the stream materialized to the topic.
	 */
	public KipesBuilder<K,V> through(String topicName) {
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		Objects.requireNonNull(topicName, "topicName");
		
		KStream<K,V> topicBackedStream = this.stream
				.through(topicName, Produced.with(
						this.keySerde, 
						this.valueSerde));
		
		return new KipesBuilder<>(
				this.streamsBuilder,
				topicBackedStream,
				this.keySerde,
				this.valueSerde,
				topicName);
	}
	
	/**
	 * Adjusts a record's timestamp. The given function must return a timestamp according to Kafka's requirements
	 * (milliseconds since 1970-01-01 00:00:00).
	 *
	 * @param evalTimestampFunction the function to evaluate the new timestamp from a given key or value.
	 * @return a new instance of {@link KipesBuilder} with the record timestamps adjusted.
	 */
	public KipesBuilder<K,V> adjustRecordTimestamps(final BiFunction<K,V, Long> evalTimestampFunction) {
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		Objects.requireNonNull(evalTimestampFunction, "evalTimestampFunction");
		
		return new KipesBuilder<>(
				this.streamsBuilder, 
				this.stream.transform(
						() -> new Transformer<K,V, KeyValue<K,V>>() {
							private ProcessorContext context;
							
							@Override
							public void init(ProcessorContext context) {
								this.context = context;
							}
		
							@Override
							public KeyValue<K, V> transform(K key, V value) {
								Long timestamp = evalTimestampFunction.apply(key, value);
								this.context.forward(key, value, To.all().withTimestamp(timestamp));
								return null;
							}
		
							@Override
							public void close() {
								// nothing to do
							}
						}), 
				this.keySerde, 
				this.valueSerde,
				this.topicsBaseName);
	}

	/**
	 * Materializes the current stream into the given topic. The topic has to be created before. <br>
	 * <p>
	 * If serdes are not set, the default serdes will be used.
	 * <p>
	 * This is a terminal operation.
	 *
	 * @param topicName the target topic.
	 */
	public void to(String topicName) {
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		Objects.requireNonNull(topicName, "topicName");
		
		this.stream
		.to(topicName, Produced.with(
				this.keySerde, 
				this.valueSerde));
	}

	/**
	 * Filters the current stream by applying the given predicate.
	 *
	 * @param predicate the {@link Predicate} to filter the current stream.
	 * @return a new initiated KipesBuilder<K,V> with the filtered stream.
	 */
	public KipesBuilder<K,V> filter(Predicate<K, V> predicate) {
		// TODO introduce FilterBuilder
		// JoinBuilder, TransactionBuilder provide the pattern. This 
		// TopologyBuild should not know the details of how this manipulation
		// works (predicate!)
		
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		Objects.requireNonNull(predicate, "predicate");
		
		return new KipesBuilder<>(
				this.streamsBuilder,
				this.stream
					.filter(predicate),
				this.keySerde,
				this.valueSerde,
				this.topicsBaseName);
	}

	/**
	 * De-duplicates the records of the current stream.<br>
	 *
	 * @param <GK> the group key type (see {@link DedupBuilder#groupBy(BiFunction, Serde)}).
	 * @return a new initiated {@link DedupBuilder} with the dedup'ed stream.
	 */
	public <GK,DV> DedupBuilder<K,V, GK,DV> dedup() {
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		
		return new DedupBuilder<K,V, GK,DV> (
				this.streamsBuilder, 
				this.stream, 
				this.keySerde, 
				this.valueSerde,
				this.topicsBaseName);
	}

	/**
	 * Creates a new KStream by (inner) joining the current stream (left) with the given other stream (right). <br>
	 * <br>
	 * The join will be backed by two changelog topics for each of the incoming records from the left and right stream.
	 * Clients have to specify the base name of these topics by calling {@link #withTopicsBaseName(String)} or
	 * {@link KipesBuilder#withTopicsBaseName(String)} at the returned JoinBuilder.<br>
	 *
	 * @param <OV>            the other (right) stream's value type.
	 * @param <VR>            the resulting stream's value type.
	 * @param otherStream     the other (right) stream.
	 * @param otherValueSerde the other stream's value {@link Serde}.
	 * @return a new initialized {@link JoinBuilder}.
	 */
	public <OV, VR> JoinBuilder<K,V, OV, VR> join(KStream<K,OV> otherStream, Serde<OV> otherValueSerde) {
		// TODO move parameters to JoinBuilder
		// TopologyBuild should not know the details of how this manipulation
		// works 
		
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		Objects.requireNonNull(this.topicsBaseName, "topicsBaseName");
		Objects.requireNonNull(otherStream, "otherStream");
		if (otherValueSerde == null) {
			LOG.warn("The default otherValueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		
		return new JoinBuilder<K,V, OV, VR>(
				this.streamsBuilder,
				this.stream, 
				this.keySerde, 
				this.valueSerde, 
				otherStream, 
				otherValueSerde,
				this.topicsBaseName);
	}

	public <OV, VR> JoinBuilder<K, V, OV, VR> join(KStream<K, OV> otherStream) {
		return join(otherStream, null);
	}

	/**
	 * Creates a new stream of TransactionRecords describing transactions found in this KipesBuilder's stream.
	 *
	 * @param <A>  actually V as A.
	 * @param <GK> the potential groupKey type, can be Void.
	 * @return a new initialized {@link TransactionBuilder}.
	 */
	@SuppressWarnings("unchecked")
	public <GK> TransactionBuilder<K,V, GK> transaction() {
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}

		return (TransactionBuilder<K,V, GK>)new TransactionBuilder<>(
				this.streamsBuilder,
				(KStream<K,V>)this.stream,
				this.keySerde,
				(Serde<V>)this.valueSerde,
				this.topicsBaseName);
	}

	/**
	 * Creates a stream of transformed records.
	 *
	 * @param <VR> the transformed record's type.
	 * @return a new initialized {@link TransformBuilder}.
	 */
	@SuppressWarnings("unchecked")
	public <KR,VR> TransformBuilder<K,V, KR,VR> transform() {
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		
		return (TransformBuilder<K,V, KR,VR>)new TransformBuilder<>(
				this.streamsBuilder, 
				this.stream, 
				this.keySerde, 
				this.valueSerde,
				this.topicsBaseName);
	}
	
	/**
	 * Creates a stream of aggregated records.
	 *
	 * @return a new initialized {@link SequenceBuilder}.
	 */
	@SuppressWarnings("unchecked")
	public <GK, VR> SequenceBuilder<K,V, GK, VR> sequence() {
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}

		return (SequenceBuilder<K,V, GK, VR>)new SequenceBuilder<>(
				this.streamsBuilder, 
				this.stream, 
				this.keySerde, 
				this.valueSerde,
				this.topicsBaseName);
	}

	/**
	 * Creates a stream from and of {@link GenericRecord}s with new fields added.
	 *
	 * @return a new initialized {@link EvalBuilder}.
	 */
	@SuppressWarnings("unchecked")
	public EvalBuilder<K> eval() {
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		
		return new EvalBuilder<>(
				this.streamsBuilder, 
				(KStream<K,GenericRecord>)this.stream, 
				this.keySerde, 
				(Serde<GenericRecord>)this.valueSerde,
				this.topicsBaseName);
	}

	/**
	 * Creates a stream from and of {@link GenericRecord}s with a field of discretized
	 * values (bins).
	 *
	 * @return a new initialized {@link BinBuilder}.
	 */
	@SuppressWarnings("unchecked")
	public BinBuilder<K> bin() {
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}

		return new BinBuilder<>(
				this.streamsBuilder,
				(KStream<K,GenericRecord>)this.stream,
				this.keySerde,
				(Serde<GenericRecord>)this.valueSerde,
				this.topicsBaseName);
	}

	/**
	 * Creates Statistics calculated based on the incoming records.
	 *
	 * @return a new initialized {@link StatsBuilder}.
	 */
	@SuppressWarnings("unchecked")
	public StatsBuilder<K> stats() {
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		
		return new StatsBuilder<>(
				this.streamsBuilder, 
				(KStream<K,GenericRecord>)this.stream, 
				this.keySerde, 
				(Serde<GenericRecord>)this.valueSerde,
				this.topicsBaseName);
		
	}
	
	/**
	 * Creates a stream Tables of incoming {@link GenericRecord}s.
	 *
	 * @return a new initialized {@link TableBuilder}.
	 */
	@SuppressWarnings("unchecked")
	public TableBuilder<K> table() {
		Objects.requireNonNull(this.stream, "stream");
		if (this.keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (this.valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		
		return new TableBuilder<>(
				this.streamsBuilder, 
				(KStream<K,GenericRecord>)this.stream, 
				this.keySerde, 
				(Serde<GenericRecord>)this.valueSerde,
				this.topicsBaseName);
	}
}