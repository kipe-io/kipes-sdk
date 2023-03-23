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

import static io.kipe.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import io.kipe.streams.recordtypes.GenericRecord;

/**
 * A Builder for calculating statistics on incoming {@link GenericRecord}s. It is not meant to be instantiated directly
 * by clients, but instead accessed through {@link KipesBuilder#stats()}.
 * <p>It allows specification of statistics functions using instances of {@link StatsExpression}, added with the
 * {@link #with(StatsExpression)} method. The target field for each function can be changed using {@link #as(String)}.
 * Grouping the statistics based on one or more fields of the GenericRecord is possible with the groupBy method. If
 * grouping is specified, the output will be a KTable with a key as a concatenated string of the group field values and
 * a value as a GenericRecord containing both grouping fields and statistics fields.
 * <p>The final topology can be assembled and started with the {@link StatsBuilder#build(Serde)} method, which returns
 * a
 * KeyValueStore with the current statistics. The output topic can be customized with {@link KipesBuilder#to(String)}.
 * <p>
 * Example:
 * <pre>{@code StreamsBuilder streamsBuilder = new StreamsBuilder();
 * KStream<String, GenericRecord> stream = streamsBuilder.stream("input-topic");
 * Serde<GenericRecord> genericRecordSerde = JsonSerdeFactory.getJsonSerde(GenericRecord.class);
 *
 * StatsBuilder<String> statsBuilder =
 *         new StatsBuilder<>(
 *                 streamsBuilder,
 *                 stream,
 *                 Serdes.String(),
 *                 genericRecordSerde,
 *                 "topic-base-name"
 *         );
 *
 * KipesBuilder<String, GenericRecord> build = statsBuilder
 *         .with(Count.count()).as("myCount")
 *         .groupBy("group")
 *         .build(Serdes.String());
 * }</pre>
 * <p>
 * In this example, a new "StreamsBuilder" object is created, and a new stream is created from the input topic named
 * "input-topic". The key and value Serde are specified as "Serdes.String()" and "genericRecordSerde" respectively. A
 * new instance of the "StatsBuilder" class is then created, using the "StreamsBuilder" object, the stream, the key and
 * value Serde, and a topic base name "topic-base-name". The statistics expression is then added using the "with"
 * method, with the target field specified as "myCount". Finally, the topology is built with a grouping based on the
 * "group" field, and the output is a KeyValueStore with a key of type "String".
 *
 * @param <K> The key type of the input Kafka topic.
 */
public class StatsBuilder<K> extends AbstractTopologyPartBuilder<K, GenericRecord> {

	private String[] groupFields = {};
	private final List<StatsExpression> expressions = new LinkedList<>();

	/**
	 * Creates a new instance of the StatsBuilder class.
	 *
	 * @param streamsBuilder the StreamsBuilder instance used to assemble the topology.
	 * @param stream         the input KStream that the topology will read from.
	 * @param keySerde       the Serde to use for the key of the input stream.
	 * @param valueSerde     the Serde to use for the value of the input stream.
	 * @param topicsBaseName the base name of the output topic. The actual topic name will be appended with a suffix.
	 */
	StatsBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, GenericRecord> stream, 
			Serde<K> keySerde, 
			Serde<GenericRecord> valueSerde,
			String topicsBaseName)
	{
		super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
	}

	/**
	 * Specifies an optional grouping based on the given fields. If there's no grouping all incoming records get
	 * aggregated into one.
	 *
	 * @param fieldNames the fields to group the aggregation by.
	 * @return this builder.
	 */
	public StatsBuilder<K> groupBy(String... fieldNames) {
		this.groupFields = fieldNames;
		return this;
	}

	/**
	 * Adds a StatsExpression. See {@link #as(String)} to override the default target field.
	 *
	 * @return this builder.
	 */
	public StatsBuilder<K> with(StatsExpression expression) {
		Objects.requireNonNull(expression, "expression");
		
		this.expressions.add(expression);
		return this;
	}

	/**
	 * Sets the target fieldName of the last aggregation function. If there was no aggregation function added before an
	 * IllegalStateException will be thrown.
	 *
	 * @param fieldName the fieldName to store the aggregation value of the last aggregation function at.
	 * @return this builder.
	 * @throws IllegalStateException if there was no expression added before.
	 * @see #with(StatsExpression)
	 */
	public StatsBuilder<K> as(String fieldName) {
		Objects.requireNonNull(fieldName, "fieldName");
		
		int numExpression = this.expressions.size();
		if(numExpression == 0) {
			throw new IllegalStateException("no aggregation function was added before");
		}
		
		this.expressions.get(numExpression-1).setFieldName(fieldName);
		
		return this;
	}

	/**
	 * Assembles the topology and emits the results as {@link KTable}. The key of each row will be a concatenated String
	 * in the form {@code {fieldValue_1}..{fieldValue_N}}. The row value will be a {@link GenericRecord} with the
	 * grouping fields and stats fields.
	 *
	 * @param keySerde a {@link Serde<String>}.
	 * @return a KeyTable holding the current stats results.
	 */
	public KTable<String, GenericRecord> asKTable(Serde<String> keySerde) {
		if (keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		Objects.requireNonNull(getTopicsBaseName(), "topicBaseName");

		return this.stream
				
				.groupBy(
						(key, value) -> {
							var sb = new StringBuilder();
							for(String field: this.groupFields) {
								sb.append("{").append(value.getString(field)).append("}");
							}
							return sb.toString();
						},
						Grouped.<String,GenericRecord>as(getTopicsBaseName())
						.withKeySerde(keySerde)
						.withValueSerde(this.valueSerde))
				
				.<GenericRecord> aggregate(
						() -> null,
						(key, value, aggregate) -> {
							
							GenericRecord a = aggregate;
							if(a == null) {
								a = new GenericRecord();
								for(String field: this.groupFields) {
									a.set(field, value.get(field));
								}								
							}
							
							for(StatsExpression e : expressions) {
								e.update(key, value, a);
							}
							
							return a;
						},
						Materialized
						.<String, GenericRecord, KeyValueStore<Bytes,byte[]>>as(getProcessorStoreTopicName(getTopicsBaseName())) 
						.withKeySerde(keySerde)
						.withValueSerde(this.valueSerde)
						.withCachingDisabled());	// disabled so that incremental aggregates are available
	}

	public KTable<String, GenericRecord> asKTable() {
		return asKTable(null);
	}
	
	/**
	 * Builds a kipes builder that contains a stream created from the KTable returned by
	 * {@link StatsBuilder#asKTable(Serde)}.
	 * <p>
	 * If a non-null value is provided for the serdes parameter, it will be used as the serde for the resulting stream.
	 * Otherwise, the default serde will be used.
	 *
	 * @param keySerde serde to use for the key of the stream.
	 * @return a kipes builder containing a stream with the specified key and value types.
	 */
	public KipesBuilder<String, GenericRecord> build(Serde<String> keySerde) {
		return createKipesBuilder(
				asKTable(keySerde)
				.toStream(),
				keySerde,
				this.valueSerde);
	}
	
	/**
	 * Builds a kipes builder that contains a stream created from the KTable returned by
	 * {@link StatsBuilder#asKTable(Serde)}.
	 * <p>
	 * It uses the default serde.
	 *
	 * @return a kipes builder containing a stream with the specified key and value types.
	 */
	public KipesBuilder<String, GenericRecord> build() {
		return build(null);
	}
}
