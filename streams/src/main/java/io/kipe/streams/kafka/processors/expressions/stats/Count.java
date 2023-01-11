package io.kipe.streams.kafka.processors.expressions.stats;

import io.kipe.streams.kafka.processors.StatsExpression;

/**
 * Stats expression to count records.
 */
// TODO update documentation
// TODO add tests
public class Count extends StatsExpression {

	public static final String DEFAULT_FIELD = "count";
	private static final Count SINGLETON = new Count();

	public static Count count() {
		return SINGLETON;
	}
	
	private Count() {
		super(DEFAULT_FIELD);
		this.valueFunction = (key, value) -> {
			Number count = value.getNumber(this.fieldName);
			return count == null? 1L : count.longValue() + 1L;
		};
	}
}
