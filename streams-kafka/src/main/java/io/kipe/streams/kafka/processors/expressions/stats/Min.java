package io.kipe.streams.kafka.processors.expressions.stats;

import io.kipe.streams.kafka.processors.StatsExpression;

/**
 * Stats expression to find the minimum value.
 */
public class Min extends StatsExpression {

    public static final String DEFAULT_FIELD = "min";

    /**
     * Returns the singleton instance of this class
     *
     * @param fieldNameToMin the field to find the min the values of
     * @return Min singleton instance
     */
    public static Min min(String fieldNameToMin) {
        return new Min(fieldNameToMin);
    }

    /**
     * Constructor for Min class, which calls the constructor of the parent class {@link StatsExpression}
     * with the default field name "min". It also sets the valueFunction in the constructor.
     */
    private Min(String fieldName) {
        super(DEFAULT_FIELD);
        this.statsFunction = (groupKey, value, aggregate) -> {
            var currentMin = aggregate.getNumber(this.fieldName);
            var newValue = value.getNumber(fieldName);
            return currentMin == null || newValue.doubleValue() < currentMin.doubleValue() ? newValue : currentMin;
        };
    }
}