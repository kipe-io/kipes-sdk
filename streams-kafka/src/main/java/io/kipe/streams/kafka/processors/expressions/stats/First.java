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
 * The First class is a stats expression that returns the first seen value of records for a specified field.
 */
public class First extends StatsExpression {

    public static final String DEFAULT_FIELD = "first";

    /**
     * Returns a new instance of this class for the specified field.
     *
     * @param fieldNameToFirst the field to get the first value of
     * @return a new First instance for the given field
     */
    public static First first(String fieldNameToFirst) {
        return new First(fieldNameToFirst);
    }

    /**
     * Initializes the statsFunction to return the first seen value of records for the specified field.
     */
    private First(String fieldNameToFirst) {
        super(DEFAULT_FIELD);
        this.statsFunction = (groupKey, value, aggregate) -> {
            var firstSeenValue = aggregate.get(fieldName);
            var fieldValue = value.get(fieldNameToFirst);
            return firstSeenValue == null ? fieldValue : firstSeenValue;
        };
    }

}
