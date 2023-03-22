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
public class StatsBuilder<K> extends AbstractTopologyPartBuilder<K, GenericRecord> {

	private String[] groupFields = {};
	private final List<StatsExpression> expressions = new LinkedList<>();
	StatsBuilder(
			StreamsBuilder streamsBuilder, 
			KStream<K, GenericRecord> stream, 
			Serde<K> keySerde, 
			Serde<GenericRecord> valueSerde,
			String topicsBaseName)
	{
		super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
	}
	public StatsBuilder<K> groupBy(String... fieldNames) {
		this.groupFields = fieldNames;
		return this;
	}
	public StatsBuilder<K> with(StatsExpression expression) {
		Objects.requireNonNull(expression, "expression");
		
		this.expressions.add(expression);
		return this;
	}
	public StatsBuilder<K> as(String fieldName) {
		Objects.requireNonNull(fieldName, "fieldName");
		
		int numExpression = this.expressions.size();
		if(numExpression == 0) {
			throw new IllegalStateException("no aggregation function was added before");
		}
		
		this.expressions.get(numExpression-1).setFieldName(fieldName);
		
		return this;
	}
	public KTable<String, GenericRecord> asKTable(Serde<String> keySerde) {
		if (keySerde == null) {
			LOG.warn("The default keySerde is being used. To customize serdes, provide a specific serde to override this behavior.");
		}
		Objects.requireNonNull(getTopicsBaseName(), "topicBaseName");

		return this.stream
				
				.groupBy(
						(key, value) -> {
							StringBuilder sb = new StringBuilder();
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
							
							for(Expression<String,GenericRecord> e : expressions) {
								e.update(key, a);
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
	public KipesBuilder<String, GenericRecord> build(Serde<String> keySerde) {
		return createKipesBuilder(
				asKTable(keySerde)
				.toStream(),
				keySerde,
				this.valueSerde);
	}
	public KipesBuilder<String, GenericRecord> build() {
		return build(null);
	}
}
