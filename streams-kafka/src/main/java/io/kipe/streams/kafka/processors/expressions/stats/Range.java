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
package io.kipe.streams.kafka.processors.expressions.stats;

import io.kipe.streams.kafka.processors.StatsExpression;

/**
 * Stats expression to calculate the range of values of records.
 * <p>
 * This class provides a Range expression and a default field name "range"
 * which will be used to store the range value in the resulting record.
 * <p>
 * The valueFunction provided in the constructor takes in a key, value, and aggregate and returns the range value
 * by first attempting to retrieve the min and max values from the provided field name in the aggregate,
 * and if they are not present, it initializes them accordingly.
 * <p>
 * The class also provides a static factory method range(..) to create an instance.
 */
public class Range extends StatsExpression {

    public static final String DEFAULT_FIELD = "range";

    /**
     * Returns a new instance of this class
     *
     * @param fieldName the field to calculate the range of
     * @return Range instance
     */
    public static Range range(String fieldName) {
        return new Range(fieldName);
    }

    private final String fieldName;

    /**
     * Constructor for Range class, which calls the constructor of the parent class {@link StatsExpression}
     * with the default field name "range". It also sets the valueFunction in the constructor.
     */
    private Range(String fieldName) {
        super(DEFAULT_FIELD);
        this.fieldName = fieldName;
        this.statsFunction = (groupKey, value, aggregate) -> {
            var min = aggregate.getNumber("min_" + this.fieldName);
            var max = aggregate.getNumber("max_" + this.fieldName);
            var fieldValue = value.getNumber(this.fieldName);

            if (min == null || fieldValue.doubleValue() < min.doubleValue()) {
                aggregate.set("min_" + this.fieldName, fieldValue);
            }

            if (max == null || fieldValue.doubleValue() > max.doubleValue()) {
                aggregate.set("max_" + this.fieldName, fieldValue);
            }

            min = aggregate.getNumber("min_" + this.fieldName);
            max = aggregate.getNumber("max_" + this.fieldName);

            return max.doubleValue() - min.doubleValue();
        };
    }
}
