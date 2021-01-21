package de.tradingpulse.streams.kafka.processors;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;

/**
 * Discretizes the values of a field into another.
 *  
 * <br>
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {SOURCE[K:V]}
 *   
 *   <b>bin</b>
 *     <b>field</b> fieldName
 *     <b>span</b> value
 *     <b>newField</b> fieldName     
 *   
 *   to
 *     {TARGET[K:V]}
 * </pre>
 * 
 * @param <K> the key type
 * @param <V> the GenericRecord type
 */
public class BinBuilder<K, V extends GenericRecord> extends AbstractTopologyPartBuilder<K, V, BinBuilder<K,V>> {

	private String fieldName;
	private Double span;
	private String newFieldName;
	
	BinBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, V> stream, 
			Serde<K> keySerde, 
			Serde<V> valueSerde) 
	{
		super(streamsBuilder, stream, keySerde, valueSerde);
	}

	public BinBuilder<K,V> field(String fieldName) {
		Objects.requireNonNull(fieldName, "fieldName");
		
		this.fieldName = fieldName;
		
		return this;
	}

	public BinBuilder<K,V> span(double span) {
		this.span = span;
		
		return this;
	}

	public BinBuilder<K,V> newField(String newFieldName) {
		
		this.newFieldName = newFieldName;
		
		return this;
	}
	
	public TopologyBuilder<K,V> build() {
		Objects.requireNonNull(this.fieldName, "fieldName");
		Objects.requireNonNull(this.span, "span");
		
		final String sourceFieldName = this.fieldName;
		final String targetFieldName = Objects.requireNonNullElse(this.newFieldName, this.fieldName);
		final double binSpan = this.span;
		
		return new EvalBuilder<>(
				this.streamsBuilder, 
				this.stream, 
				this.keySerde, 
				this.valueSerde)
				.with(
						targetFieldName, 
						(key,value) -> Math.round(value.getDouble(sourceFieldName) / binSpan) * binSpan )
				.build();
	}
	
}
