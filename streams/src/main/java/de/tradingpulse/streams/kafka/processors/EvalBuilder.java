package de.tradingpulse.streams.kafka.processors;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;

/**
 * <br>
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {SOURCE[K:GenericRecord]}
 *   
 *   <b>eval</b>
 *     ({FIELD} = {EXPRESSION})+
 *   
 *   to
 *     {TARGET[K:GenericRecord]}
 * </pre>
 * 
 * @param <K> the key type
 */
public class EvalBuilder<K> extends AbstractTopologyPartBuilder<K, GenericRecord> {

	private final List<Expression<K,GenericRecord>> expressions = new LinkedList<>();
	
	EvalBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, GenericRecord> stream, 
			Serde<K> keySerde, 
			Serde<GenericRecord> valueSerde,
			String topicsBaseName) 
	{
		super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
	}

	public EvalBuilder<K> with(String fieldName, BiFunction<K, GenericRecord, Object> valueFunction) {
		Objects.requireNonNull(fieldName, "fieldName");
		Objects.requireNonNull(valueFunction, "valueFunction");
		
		this.expressions.add(new Expression<>(fieldName, valueFunction));
		
		return this;
	}
	
	public TopologyBuilder<K, GenericRecord> build() {
		return createTopologyBuilder(
				this.stream
				.map(
						(key, value) -> {
							this.expressions.forEach(expression -> expression.update(key, value));
							return new KeyValue<>(key, value);
						}));
	}

}
