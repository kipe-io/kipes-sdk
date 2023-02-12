package io.kipe.streams.kafka.processors;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import io.kipe.common.utils.MathUtils;
import io.kipe.streams.recordtypes.GenericRecord;


/**
 * Discretizes the values of a field into another by using the binning technique. The binning technique
 * groups the continuous values of a field into a fixed number of discrete bins.
 * <p>
 * This class provides a fluent API to define the binning configuration, including the field name to
 * discretize, the bin span and the new field name.
 * <p>
 * <b>Usage:</b>
 * To use this class, you first need to create an instance of it by passing the required parameters such as
 * `StreamsBuilder`, `KStream`, key and value `Serde`, and a `topicsBaseName`. Then, you can call the following
 * methods to set the binning configuration:
 * <ul>
 *   <li>{@link #field(String)} to set the field to be binned.</li>
 *   <li>{@link #span(double)} to set the bin span.</li>
 *   <li>{@link #newField(String)} to set the new field name for the binned field. If not set, the original field name will be used.</li>
 * </ul>
 * Finally, you can call the {@link #build()} method to build the topology that performs the binning operation and returns a
 * {@link KipesBuilder} object.
 * <p>
 * <b>Example:</b>
 * For example, the following code snippet shows how to use this class to discretize a field called
 * "price" into bins with a span of 10 and store the result in a new field called "price_bin":
 * <pre>
 * {@code
 * BinBuilder<String> binBuilder = new BinBuilder<>(streamsBuilder, stream, keySerde, valueSerde, "topicName");
 * binBuilder.field("price").span(10).newField("price_bin").build();
 * }
 * </pre>
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

	/**
	 * Creates a new instance of BinBuilder.
	 *
	 * @param streamsBuilder The StreamsBuilder used to build the topology.
	 * @param stream         The input KStream to be binned.
	 * @param keySerde       The Serde used for the key in the input stream.
	 * @param valueSerde     The Serde used for the value in the input stream.
	 * @param topicsBaseName The base name used for the topics in the topology.
	 */
	BinBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, GenericRecord> stream, 
			Serde<K> keySerde, 
			Serde<GenericRecord> valueSerde,
			String topicsBaseName) 
	{
		super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
	}

	/**
	 * Sets the field to be binned.
	 *
	 * @param fieldName The name of the field to be binned.
	 * @return The current BinBuilder instance.
	 */
	public BinBuilder<K> field(String fieldName) {
		Objects.requireNonNull(fieldName, "fieldName");
		
		this.fieldName = fieldName;
		
		return this;
	}

	/**
	 * Sets the bin span.
	 *
	 * @param span The bin span.
	 * @return The current BinBuilder instance.
	 */
	public BinBuilder<K> span(double span) {
		this.span = span;
		
		return this;
	}


	/**
	 * Sets the new field name for the binned field.
	 * <p>
	 * If not set, the original field name will be used.
	 *
	 * @param newFieldName The new field name for the binned field.
	 * @return The current BinBuilder instance.
	 */
	public BinBuilder<K> newField(String newFieldName) {
		
		this.newFieldName = newFieldName;
		
		return this;
	}

	/**
	 * Builds the topology for binning the input stream.
	 *
	 * @return The KipesBuilder for the binned stream.
	 * @throws IllegalStateException if fieldName or span have not been set.
	 */
	public KipesBuilder<K,GenericRecord> build() {
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
