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
 * The DistinctCount class counts distinct values of a specified field in a data stream.
 */
public class DistinctCount extends StatsExpression {

    public static final String DEFAULT_FIELD = "distinct_count";

    /**
     * Returns a new DistinctCount instance for the specified field.
     *
     * @param fieldNameToDistinctCount the field to count distinct values of
     * @return a new DistinctCount instance for the given field
     */
    public static DistinctCount distinctCount(String fieldNameToDistinctCount) {
        return new DistinctCount(fieldNameToDistinctCount);
    }

    /**
     * Initializes the statsFunction to count distinct values of the specified field by maintaining a set of unique
     * values.
     */
    private DistinctCount(String fieldNameToDistinctCount) {
        super(DEFAULT_FIELD);
        this.statsFunction = (groupKey, value, aggregate) -> {
            Set<Object> uniqueValues = aggregate.get(fieldNameToDistinctCount);
            if (uniqueValues == null) uniqueValues = new HashSet<>();
            uniqueValues.add(value.get(fieldNameToDistinctCount));
            aggregate.set(fieldNameToDistinctCount, uniqueValues);
            return uniqueValues.size();
        };
    }

}
