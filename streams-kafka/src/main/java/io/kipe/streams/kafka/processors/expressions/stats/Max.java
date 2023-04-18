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
 * Stats expression to find the maximum value.
 * <p>
 * The fields for this statistical expression are as follows:
 * <pre>
 * | field | internal | type   | description                             |
 * |-------|----------|--------|-----------------------------------------|
 * | max   | no       | double | the maximum value of the measured field |
 * </pre>
 */
public class Max extends StatsExpression {

    public static final String DEFAULT_FIELD = "max";

    /**
     * Returns the singleton instance of this class
     *
     * @param fieldNameToMax the field to find the max the values of
     * @return Max singleton instance
     */
    public static Max max(String fieldNameToMax) {
        return new Max(fieldNameToMax);
    }

    /**
     * Constructor for Max class, which calls the constructor of the parent class {@link StatsExpression}
     * with the default field name "max". It also sets the valueFunction
     * in the constructor.
     */
    private Max(String fieldNameToMax) {
        super(DEFAULT_FIELD);
        this.statsFunction = (groupKey, value, aggregate) -> {
            Number fieldValue = value.getNumber(fieldNameToMax);
            if (fieldValue == null) {
                return aggregate.getNumber(this.fieldName);
            }

            Number currentMax = aggregate.get(this.fieldName, () -> fieldValue);
            if (fieldValue.doubleValue() > currentMax.doubleValue()) {
                aggregate.set(this.fieldName, fieldValue);
                return fieldValue;
            }
            return currentMax;
        };
    }
}