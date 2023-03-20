package io.kipe.streams.kafka.processors;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.LoggerFactory;

/**
 * AbstractTopologyPartBuilder is a base class for building Kafka Streams topology parts.
 * <p>
 * It provides utility methods for creating {@link KipesBuilder} instances and accessing
 * shared properties such as the {@link StreamsBuilder} and the key and value {@link Serde}.
 *
 * @param <K> the key type of the input stream.
 * @param <V> the value type of the input stream.
 */
abstract class AbstractTopologyPartBuilder<K,V> {
	
	static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AbstractTopologyPartBuilder.class);

	protected final StreamsBuilder streamsBuilder;
	protected final KStream<K,V> stream;
	protected final Serde<K> keySerde;
	protected final Serde<V> valueSerde;
	protected final String topicsBaseName;

	/**
	 * Creates a new instance of the AbstractTopologyPartBuilder with the provided properties.
	 *
	 * @param streamsBuilder the {@link StreamsBuilder} used to build the topology.
	 * @param stream         the {@link KStream} for the input stream.
	 * @param keySerde       the {@link Serde} for the key of the input stream.
	 * @param valueSerde     the {@link Serde} for the value of the input stream.
	 * @param topicsBaseName the base name of the topics used in the topology.
	 * @throws NullPointerException if any of the arguments are null.
	 */
	AbstractTopologyPartBuilder(
			StreamsBuilder streamsBuilder,
			KStream<K, V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde,
			String topicsBaseName)
	{
		Objects.requireNonNull(streamsBuilder, "streamsBuilder");
		Objects.requireNonNull(stream, "stream");
		if (keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		if (valueSerde == null) {
			LOG.warn("The default valueSerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		
		this.streamsBuilder = streamsBuilder;
		this.stream = stream;
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;
		this.topicsBaseName = topicsBaseName; 
	}


	/**
	 * Returns the base name for the topics that this topology builder is using.
	 *
	 * @return the base name for the topics.
	 */
	protected String getTopicsBaseName() {
		return this.topicsBaseName;
	}

	/**
	 * Creates a new instance of {@link KipesBuilder} for the specified input stream.
	 *
	 * If the builder's key and value serdes are not set, it falls back to the default serdes.
	 *
	 * @param stream the input stream to create the topology builder for
	 * @return a new instance of {@link KipesBuilder}.
	 */
	protected KipesBuilder<K,V> createKipesBuilder(KStream<K, V> stream) {
		return KipesBuilder.init(this.streamsBuilder)
				.from(
						stream, 
						this.keySerde, 
						this.valueSerde)
				.withTopicsBaseName(this.topicsBaseName);
	}

	/**
	 * Returns a new instance of {@link KipesBuilder} for the input stream with the shared topics base name.
	 *
	 * @param stream     the input stream to create the topology builder from.
	 * @param keySerde   the key serde to use for the input stream.
	 * @param valueSerde the value serde to use for the input stream.
	 * @return a new instance of {@link KipesBuilder} for the input stream with the shared topics base name.
	 */
	protected <KR,VR> KipesBuilder<KR,VR> createKipesBuilder(
			KStream<KR, VR> stream,
			Serde<KR> keySerde,
			Serde<VR> valueSerde)
	{
		return KipesBuilder.init(this.streamsBuilder)
				.from(
						stream, 
						keySerde, 
						valueSerde)
				.withTopicsBaseName(this.topicsBaseName);
	}
}
