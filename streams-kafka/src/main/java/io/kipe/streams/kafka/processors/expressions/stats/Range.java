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
 * The Range class calculates the range of values in a data stream by finding the difference between the maximum and
 * minimum values of a specified field.
 * <p>
 * The fields for this statistical expression are as follows:
 * <pre>
 * | field | internal | type   | description                                          |
 * |-------|----------|--------|------------------------------------------------------|
 * | range | no       | double | the calculated range of values in the measured field |
 * | min   | yes      | double | the minimum value found in the measured field        |
 * | max   | yes      | double | the maximum value found in the measured field        |
 * </pre>
 */
public class Range extends StatsExpression {
    public static final String DEFAULT_FIELD = "range";

    /**
     * Returns a new Range instance for the specified field.
     *
     * @param fieldNameToRange the field for which the range will be calculated
     * @return a new Range instance for the given field
     */
    public static Range range(String fieldNameToRange) {
        return new Range(fieldNameToRange);
    }

    /**
     * Initializes the statsFunction to calculate the range by finding the minimum and maximum values for the specified
     * field and computing the difference between them.
     */
    private Range(String fieldNameToRange) {
        super(DEFAULT_FIELD);
        this.statsFunction = (groupKey, value, aggregate) -> {
            String fieldNameMin = createInternalFieldName("min");
            String fieldNameMax = createInternalFieldName("max");

            Number min = aggregate.getNumber(fieldNameMin);
            Number max = aggregate.getNumber(fieldNameMax);
            Number fieldValue = value.getNumber(fieldNameToRange);

            if (fieldValue == null) {
                return min != null && max != null ? max.doubleValue() - min.doubleValue() : null;
            }

            if (min == null || fieldValue.doubleValue() < min.doubleValue()) {
                aggregate.set(fieldNameMin, fieldValue);
            }

            if (max == null || fieldValue.doubleValue() > max.doubleValue()) {
                aggregate.set(fieldNameMax, fieldValue);
            }

            min = aggregate.getNumber(fieldNameMin);
            max = aggregate.getNumber(fieldNameMax);

            return max.doubleValue() - min.doubleValue();
        };
    }
}
