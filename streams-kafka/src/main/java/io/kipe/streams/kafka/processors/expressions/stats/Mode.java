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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The Mode class calculates the mode of values in a data stream by finding the most frequently occurring value(s) in a
 * specified field.
 * <p>
 * The fields for this statistical expression are as follows:
 * <pre>
 * | field  | internal | type                     | description                                      |
 * |--------|----------|--------------------------|--------------------------------------------------|
 * | mode   | no       | set of strings           | the mode of values at the measured value field   |
 * | counts | yes      | map of string to integer | the frequency of each unique value               |
 * </pre>
 */
public class Mode extends StatsExpression {

    public static final String DEFAULT_FIELD = "mode";

    /**
     * Returns a new Mode instance for the specified field.
     *
     * @param fieldNameToMode the field for which the mode will be calculated
     * @return a new Mode instance for the given field
     */
    public static Mode mode(String fieldNameToMode) {
        return new Mode(fieldNameToMode);
    }

    /**
     * Initializes the statsFunction to calculate the mode by counting the frequency of each unique value for the
     * specified field and determining the most frequently occurring value(s).
     */
    private Mode(String fieldNameToMode) {
        super(DEFAULT_FIELD);
        this.statsFunction = (groupKey, value, aggregate) -> {
            String fieldNameCounts = createInternalFieldName("counts");

            Map<String, Integer> counts = aggregate.getMap(fieldNameCounts);
            if (counts == null) {
                counts = new HashMap<>();
                aggregate.set(fieldNameCounts, counts);
            }

            String fieldValue = value.getString(fieldNameToMode);
            counts.put(fieldValue, counts.getOrDefault(fieldValue, 0) + 1);

            return calculateModes(counts);
        };
    }

    private Set<String> calculateModes(Map<String, Integer> counts) {
        int maxCount = counts.values().stream().max(Integer::compareTo).orElse(0);

        return counts.entrySet().stream()
                .filter(entry -> entry.getValue() == maxCount)
                .map(Entry::getKey)
                .collect(Collectors.toSet());
    }

}
