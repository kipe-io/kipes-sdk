package io.kipe.streams.kafka.processors;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

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
		Objects.requireNonNull(keySerde, "keySerde");
		Objects.requireNonNull(valueSerde, "valueSerde");
		
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
	 * Creates a new instance of {@link KipesBuilder} for the input stream with the shared
	 * key and value serdes and the shared topics base name.
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
