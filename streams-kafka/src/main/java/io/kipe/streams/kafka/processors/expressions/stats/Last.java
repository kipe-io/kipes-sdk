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
 * The Last class is a stats expression that returns the last seen value of records for a specified field.
 * <p>
 * The fields for this statistical expression are as follows:
 * <pre>
 * | field | internal | type   | description                                     |
 * |-------|----------|--------|-------------------------------------------------|
 * | last  | no       | object | the last seen value at the measured value field |
 * </pre>
 */
public class Last extends StatsExpression {

    public static final String DEFAULT_FIELD = "last";

    /**
     * Returns a new instance of this class for the specified field.
     *
     * @param fieldNameToLast the field to get the last value of
     * @return a new Last instance for the given field
     */
    public static Last last(String fieldNameToLast) {
        return new Last(fieldNameToLast);
    }

    /**
     * Initializes the statsFunction to return the last seen value of records for the specified field.
     */
    private Last(String fieldNameToLast) {
        super(DEFAULT_FIELD);
        this.statsFunction = (groupKey, value, aggregate) -> {
            var fieldValue = value.get(fieldNameToLast);
            return fieldValue == null ? aggregate.get(fieldName) : fieldValue;
        };
    }
}
