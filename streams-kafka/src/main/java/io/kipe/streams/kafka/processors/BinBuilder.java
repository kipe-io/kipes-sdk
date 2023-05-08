/*
 * Kipes SDK for Kafka - The High-Level Event Processing SDK.
 * Copyright © 2023 kipe.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.kipe.streams.kafka.processors;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import io.kipe.common.utils.MathUtils;
import io.kipe.streams.recordtypes.GenericRecord;


/**
 * A builder for setting up the discretization of field values using the binning techniqu.. Clients do not instantiate
 * this class directly but use {@link KipesBuilder#bin()}.
 * <p>
 * Discretizes the continuous values of a field into fixed bins, providing a fluent API for defining binning
 * configuration such as the field name, bin span, and new field name. To use, create an instance of BinBuilder with the
 * required parameters, then set the binning configuration using field(), span(), and newField(), and finally call
 * build() to build the topology.
 * <b>Example:</b>
 * <pre>{@code
 * BinBuilder<String> binBuilder = new BinBuilder<>(
 *         streamsBuilder,
 *         binStream,
 *         Serdes.String(),
 *         genericRecordSerde,
 *         "topicName"
 * );
 * binBuilder
 *         .field("price")
 *         .span(10)
 *         .newField("price_bin")
 *         .build();}</pre>
 * <br>
 * In this example, the BinBuilder class is utilized to perform binning on the values of a field. The BinBuilder is
 * created by passing in a StreamsBuilder object, a KStream, a key Serde and a value Serde, and a topic name. The field
 * to be binned is specified using the .field("price") method, the bin span is set with .span(10), and the name of the
 * new field is defined with .newField("price_bin"). Finally, the .build() method is called to construct the topology
 * for the binning operation, which returns a KipesBuilder object.
 * <p>
 * The table below shows the bin command with its stateful and internal topics details:
 * <pre>
 * | command | stateful | internal topics |
 * |---------|----------|-----------------|
 * | bin     | no       | -               |
 * </pre>
 *
 * @param <K> the key type.
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
