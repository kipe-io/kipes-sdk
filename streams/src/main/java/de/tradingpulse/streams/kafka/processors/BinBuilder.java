package de.tradingpulse.streams.kafka.processors;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.utils.MathUtils;
import de.tradingpulse.streams.recordtypes.GenericRecord;

/**
 * Discretizes the values of a field into another.
 *  
 * <br>
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {SOURCE[K:GenericRecord]}
 *   
 *   <b>bin</b>
 *     <b>field</b> fieldName
 *     <b>span</b> value
 *     <b>newField</b> fieldName     
 *   
 *   to
 *     {TARGET[K:GenericRecord]}
 * </pre>
 * 
 * @param <K> the key type
 */
public class BinBuilder<K> extends AbstractTopologyPartBuilder<K, GenericRecord> {

	private String fieldName;
	private Double span;
	private String newFieldName;
	
	BinBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, GenericRecord> stream, 
			Serde<K> keySerde, 
			Serde<GenericRecord> valueSerde,
			String topicsBaseName) 
	{
		super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
	}

	public BinBuilder<K> field(String fieldName) {
		Objects.requireNonNull(fieldName, "fieldName");
		
		this.fieldName = fieldName;
		
		return this;
	}

	public BinBuilder<K> span(double span) {
		this.span = span;
		
		return this;
	}

	public BinBuilder<K> newField(String newFieldName) {
		
		this.newFieldName = newFieldName;
		
		return this;
	}
	
	public TopologyBuilder<K,GenericRecord> build() {
		Objects.requireNonNull(this.fieldName, "fieldName");
		Objects.requireNonNull(this.span, "span");
		
		final String sourceFieldName = this.fieldName;
		final String targetFieldName = Objects.requireNonNullElse(this.newFieldName, this.fieldName);
		final double binSpan = this.span;
		
		return new EvalBuilder<>(
				this.streamsBuilder, 
				this.stream, 
				this.keySerde, 
				this.valueSerde,
				this.topicsBaseName)
				.with(
						targetFieldName, 
						(key,value) -> 
							MathUtils.round(
								MathUtils.round(value.getDouble(sourceFieldName) / binSpan, 0)
								* binSpan, MathUtils.getPrecision(binSpan)))
				.build();
	}
	
}
