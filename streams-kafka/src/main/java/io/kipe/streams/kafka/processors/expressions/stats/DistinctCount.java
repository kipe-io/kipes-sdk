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

import java.util.HashSet;
import java.util.Set;

/**
 * Stats expression to count distinct values of records.
 * <p>
 * This class provides a default field name "distinct_count"
 * which will be used to store the distinct count value in the resulting record.
 * <p>
 * The valueFunction provided in the constructor takes in a key and value and returns the distinct count value
 * by maintaining a set of unique values of the specified field.
 * <p>
 * The class also provides a static factory method distinctCount(..) to create an instance.
 */
public class DistinctCount extends StatsExpression {

    public static final String DEFAULT_FIELD = "distinct_count";

    /**
     * Returns a new instance of this class
     *
     * @param fieldNameToDistinctCount the field to count distinct values of
     * @return DistinctCount instance
     */
    public static DistinctCount distinctCount(String fieldNameToDistinctCount) {
        return new DistinctCount(fieldNameToDistinctCount);
    }

    private final String fieldName;

    /**
     * Constructor for DistinctCount class, which calls the constructor of the parent class {@link StatsExpression}
     * with the default field name "distinct_count". It also sets the valueFunction in the constructor.
     */
    private DistinctCount(String fieldNameToDistinctCount) {
        super(DEFAULT_FIELD);
        this.fieldName = fieldNameToDistinctCount;
        this.statsFunction = (groupKey, value, aggregate) -> {
            Set<Object> uniqueValues = aggregate.get(this.fieldName);
            if (uniqueValues == null) uniqueValues = new HashSet<>();
            uniqueValues.add(value.get(fieldNameToDistinctCount));
            aggregate.set(this.fieldName, uniqueValues);
            return uniqueValues.size();
        };
    }

}
