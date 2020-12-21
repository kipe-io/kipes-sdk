package de.tradingpulse.streams.kafka.processors;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

abstract class AbstractTopologyPartBuilder<K,V, B extends AbstractTopologyPartBuilder<K, V, B>> {

	protected final StreamsBuilder streamsBuilder;
	protected final KStream<K,V> stream;
	protected final Serde<K> keySerde;
	protected final Serde<V> valueSerde;

	private String topicsBaseName;

	/**
	 * All values must be non-null, otherwise a NullPointerException will be 
	 * thrown. 
	 * 
	 * @param streamsBuilder
	 * @param stream
	 * @param keySerde
	 * @param valueSerde
	 */
	AbstractTopologyPartBuilder(
			StreamsBuilder streamsBuilder,
			KStream<K, V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde)
	{
		Objects.requireNonNull(streamsBuilder, "streamsBuilder");
		Objects.requireNonNull(stream, "stream");
		Objects.requireNonNull(keySerde, "keySerde");
		Objects.requireNonNull(valueSerde, "valueSerde");
		
		this.streamsBuilder = streamsBuilder;
		this.stream = stream;
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;
	}
	
	@SuppressWarnings("unchecked")
	public final B withTopicsBaseName(String topicsBaseName) {
		this.topicsBaseName = topicsBaseName;
		return (B)this;
	}
	
	protected String getTopicsBaseName() {
		return this.topicsBaseName;
	}
	
	protected <KR,VR> TopologyBuilder<KR,VR> createTopologyBuilder(
			KStream<KR, VR> stream,
			Serde<KR> keySerde,
			Serde<VR> valueSerde)
	{
		return TopologyBuilder.init(this.streamsBuilder)
				.from(
						stream, 
						keySerde, 
						valueSerde)
				.withTopicsBaseName(this.topicsBaseName);
	}
}
