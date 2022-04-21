package de.tradingpulse.streams.kafka.processors;

import de.tradingpulse.streams.recordtypes.GenericRecord;

public abstract class StatsExpression extends Expression<String, GenericRecord> {

	protected StatsExpression(String defaultFieldName) {
		this.fieldName = defaultFieldName;
	}
}
