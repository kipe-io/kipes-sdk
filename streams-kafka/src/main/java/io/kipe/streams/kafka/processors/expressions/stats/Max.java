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
 */
public class Max extends StatsExpression {

    public static final String DEFAULT_FIELD = "max";

    /**
     * Returns the singleton instance of this class
     *
     * @param fieldNameToMax the field to find the max the values of
     * @return Min singleton instance
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
            var currentMax = aggregate.getNumber(this.fieldName);
            var newValue = value.getNumber(fieldNameToMax);
            return currentMax == null || newValue.doubleValue() > currentMax.doubleValue() ? newValue : currentMax;
        };
    }
}