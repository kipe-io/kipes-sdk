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
 * Stats expression to return the first seen value of records.
 * <p>
 * This class provides a singleton instance of the First expression and a default field name "first"
 * which will be used to store the first seen value in the resulting record.
 * <p>
 * The valueFunction provided in the constructor takes in a key and value and returns the first seen value
 * by first attempting to retrieve the value from the provided field name in the value,
 * and if it is not present, it defaults to the first seen value.
 * <p>
 * The class also provides a static factory method first(..) to retrieve an instance.
 */
public class First extends StatsExpression {

    public static final String DEFAULT_FIELD = "first";

    /**
     * Returns the singleton instance of this class
     *
     * @param fieldNameToFirst the field to get the first value of
     * @return First singleton instance
     */
    public static First first(String fieldNameToFirst) {
        return new First(fieldNameToFirst);
    }

    private final String fieldNameToFirst;

    /**
     * Constructor for First class, which calls the constructor of the parent class {@link StatsExpression}
     * with the default field name "first". It also sets the valueFunction in the constructor.
     */
    private First(String fieldNameToFirst) {
        super(DEFAULT_FIELD);
        this.fieldNameToFirst = fieldNameToFirst;
        this.statsFunction = (groupKey, value, aggregate) -> {
            var firstSeenValue = aggregate.get(fieldName);
            var fieldValue = value.get(this.fieldNameToFirst);
            return firstSeenValue == null ? fieldValue : firstSeenValue;
        };
    }

}
