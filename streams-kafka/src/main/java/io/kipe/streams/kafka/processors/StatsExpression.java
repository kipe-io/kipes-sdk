package io.kipe.streams.kafka.processors;

import io.kipe.streams.recordtypes.GenericRecord;

/**
 * An Abstract class for defining statistics expressions to be applied to Kafka records.
 */
public abstract class StatsExpression extends Expression<String, GenericRecord> {

	/**
	 * Constructor for creating a {@link StatsExpression}.
	 *
	 * @param defaultFieldName The default field name to be used by the
	 *                         expression.
	 */
	protected StatsExpression(String defaultFieldName) {
		this.fieldName = defaultFieldName;
	}
}
