package io.kipe.streams.kafka.processors;

import io.kipe.streams.recordtypes.GenericRecord;

/**
 * Abstract class for defining statistics expressions to be applied to Kafka records.
 *
 * @param <String>        The type of the key in the Kafka record.
 * @param <GenericRecord> The type of the value in the Kafka record.
 */
public abstract class StatsExpression extends Expression<String, GenericRecord> {

	/**
	 * Constructor for creating a {@link StatsExpression}.
	 *
	 * @param defaultFieldName The default field name to be used by the expression.
	 */
	protected StatsExpression(String defaultFieldName) {
		this.fieldName = defaultFieldName;
	}
}
