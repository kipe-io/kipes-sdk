package de.tradingpulse.streams.kafka.processors;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;

public abstract class StatsExpression extends Expression<String, GenericRecord> {

	protected StatsExpression(String defaultFieldName) {
		this.fieldName = defaultFieldName;
	}
}
