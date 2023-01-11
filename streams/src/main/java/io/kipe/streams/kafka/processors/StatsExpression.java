package io.kipe.streams.kafka.processors;

import io.kipe.streams.recordtypes.GenericRecord;

public abstract class StatsExpression extends Expression<String, GenericRecord> {

	protected StatsExpression(String defaultFieldName) {
		this.fieldName = defaultFieldName;
	}
}
