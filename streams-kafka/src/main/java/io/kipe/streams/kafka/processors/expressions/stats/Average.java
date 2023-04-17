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
 * The Average class calculates the average value of a specified field within a dataset.
 * <p>
 * The fields for this statistical expression are as follows:
 * <pre>
 * | field | internal | type    | description                                         |
 * |-------|----------|---------|-----------------------------------------------------|
 * | avg   | no       | double  | the calculated average value of the measured field  |
 * | sum   | yes      | double  | the running sum of the values in the measured field |
 * | count | yes      | integer | the running count of values in the measured field   |
 * </pre>
 */
public class Average extends StatsExpression {

    public static final String DEFAULT_FIELD = "avg";

    /**
     * Returns a new {@code Average} instance configured to compute the average value for the specified field name.
     *
     * @param fieldNameToAverage the field name for which to find the average value
     * @return a new {@code Average} instance
     */
    public static Average average(String fieldNameToAverage) {
        return new Average(fieldNameToAverage);
    }

    /**
     * Initializes the statsFunction to compute the average value for the specified field by maintaining a running sum
     * and count of values.
     *
     * @param fieldNameToAverage the field name for which to find the average value
     */
    private Average(String fieldNameToAverage) {
        super(DEFAULT_FIELD);
        this.statsFunction = (groupKey, value, aggregate) -> {
            String fieldNameSum = createInternalFieldName("sum");
            String fieldNameCount = createInternalFieldName("count");

            Number currentSumNumber = aggregate.getNumber(fieldNameSum);
            Number currentCountNumber = aggregate.getNumber(fieldNameCount);
            Number fieldValue = value.getNumber(fieldNameToAverage);

            double currentSum = (currentSumNumber != null) ? currentSumNumber.doubleValue() : 0.0;
            int currentCount = (currentCountNumber != null) ? currentCountNumber.intValue() : 0;

            if (fieldValue != null) {
                double newValue = fieldValue.doubleValue();
                currentSum += newValue;
                currentCount++;

                aggregate.set(fieldNameSum, currentSum);
                aggregate.set(fieldNameCount, currentCount);
            } else {
                return aggregate.getNumber(this.fieldName);
            }

            return currentCount > 0 ? currentSum / currentCount : 0;
        };
    }
}
