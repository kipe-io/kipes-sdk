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
 * Calculates statistics of incoming GenericRecords.
 *  
 * <br>
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {SOURCE[K:GenericRecord]}
 *   
 *   <b>stats</b>
 *     (<b>{STATS_FUNCTION}</b> as fieldName)+
 *     <b>by</b> (fieldName)+     
 *   
 *   to
 *     {TARGET[String:GenericRecord]}
 * </pre>
 * 
 * @param <K> the key type
 */
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

	/**
	 * Specifies an optional grouping based on the given fields. If there's no 
	 * grouping all incoming records get aggregated into one.  
	 * 
	 * @param fieldNames the fields to group the aggregation by
	 * 
	 * @return
	 * 	this builder
	 */
	public StatsBuilder<K> groupBy(String... fieldNames) {
		this.groupFields = fieldNames;
		return this;
	}
	
	/**
	 * Adds a StatsExpression. 
	 * See {@link #as(String)} to override the default target field.
	 *  
	 * @return
	 * 	this builder
	 */
	public StatsBuilder<K> with(StatsExpression expression) {
		Objects.requireNonNull(expression, "expression");
		
		this.expressions.add(expression);
		return this;
	}
	
	/**
	 * Sets the target fieldName of the last aggregation function. If there
	 * was no aggregation function added before an IllegalStateException will
	 * be thrown.
	 * 
	 * @param fieldName the fieldName to store the aggregation value of the last aggregation function at
	 * 
	 * @return
	 * 	this builder
	 * 
	 * @throws IllegalStateException if there was no expression added before
	 * 
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
	 * Assembles the topology and emits the results as KTable. The key of each
	 * row will be a concatenated String in the form 
	 * "{fieldValue_1}..{fieldValue_N}". The row value will be a GenericRecord
	 * with the grouping fields and stats fields.
	 * 
	 * @param keySerde a Serde<String>
	 * 
	 * @return
	 * 	a KeyTable holding the current stats results.
	 */
	public KTable<String, GenericRecord> asKTable(Serde<String> keySerde) {
		Objects.requireNonNull(keySerde, "keySerde");
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
	
	public TopologyBuilder<String, GenericRecord> build(Serde<String> keySerde) {
		return createTopologyBuilder(
				asKTable(keySerde)
				.toStream(),
				keySerde,
				this.valueSerde);
	}
}
