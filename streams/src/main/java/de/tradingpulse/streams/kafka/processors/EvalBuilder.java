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
import lombok.AllArgsConstructor;

/**
 * <br>
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {SOURCE[key:{GenericRecord}]}
 *   
 *   <b>eval</b>
 *     ({FIELD} = {EXPRESSION})+
 *   
 *   to
 *     {TARGET[key:{GenericRecord}]}
 * </pre>
 * 
 * @param <K>
 * @param <V>
 */
public class EvalBuilder<K, V extends GenericRecord> extends AbstractTopologyPartBuilder<K, V, EvalBuilder<K, V>> {

	private final List<Expression<K,V>> expressions = new LinkedList<>();
	
	EvalBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde) 
	{
		super(streamsBuilder, stream, keySerde, valueSerde);
	}

	public EvalBuilder<K,V> with(String fieldName, BiFunction<K, V, Object> valueFunction) {
		Objects.requireNonNull(fieldName, "fieldName");
		Objects.requireNonNull(valueFunction, "valueFunction");
		
		this.expressions.add(new Expression<K,V>(fieldName, valueFunction));
		
		return this;
	}
	
	public TopologyBuilder<K, V> build() {
		return createTopologyBuilder(
				this.stream
				.map(
						(key, value) -> {
							this.expressions.forEach(expression -> expression.update(key, value));
							return new KeyValue<>(key, value);
						}));
	}
	
	// ------------------------------------------------------------------------
	// Expression
	// ------------------------------------------------------------------------

	@AllArgsConstructor
	static class Expression<K, V extends GenericRecord> {
		
		private final String fieldName;
		private final BiFunction<K, V, Object> valueFunction;
		
		void update(K key, V record) {
			record.set(
					this.fieldName, 
					this.valueFunction.apply(key, record));
		}
	}
}
