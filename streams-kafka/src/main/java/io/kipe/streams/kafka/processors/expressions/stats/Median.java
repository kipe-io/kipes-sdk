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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The Median class calculates the median value of a data stream for a specified field.
 */
public class Median extends StatsExpression {
    public static final String DEFAULT_FIELD = "median";

    /**
     * Returns a new Median instance for the specified field.
     *
     * @param fieldNameToMedian the field for which the median will be calculated
     * @return a new Median instance for the given field
     */
    public static Median median(String fieldNameToMedian) {
        return new Median(fieldNameToMedian);
    }

    /**
     * Initializes the statsFunction to calculate the median by collecting the values for the specified field and
     * finding the middle value when sorted.
     */
    private Median(String fieldNameToMedian) {
        super(DEFAULT_FIELD);
        this.statsFunction = (groupKey, value, aggregate) -> {
            String fieldNameValues = createInternalFieldName("values");

            List<Number> values = aggregate.get(fieldNameValues);
            if (values == null) {
                values = new ArrayList<>();
            }

            values.add(value.getNumber(fieldNameToMedian));
            Collections.sort(values, (a, b) -> Double.compare(a.doubleValue(), b.doubleValue()));

            aggregate.set(fieldNameValues, values);

            int size = values.size();
            if (size == 0) {
                return 0.0;
            } else if (size % 2 == 0) {
                return (values.get(size / 2 - 1).doubleValue() + values.get(size / 2).doubleValue()) / 2;
            } else {
                return values.get(size / 2).doubleValue();
            }
        };
    }
}
