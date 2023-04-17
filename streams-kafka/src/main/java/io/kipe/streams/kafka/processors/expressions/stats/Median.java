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

import com.google.common.collect.TreeMultiset;
import io.kipe.streams.kafka.processors.StatsExpression;

/**
 * The Median class calculates the median value of a data stream for a specified field.
 * <p>
 * The fields for this statistical expression are as follows:
 * <pre>
 * | field     | internal | type               | description                                       |
 * |-----------|----------|--------------------|---------------------------------------------------|
 * | median    | no       | double             | the calculated median value of the measured field |
 * | lowerHalf | yes      | multiset of double | the lower half values for the specified field     |
 * | upperHalf | yes      | multiset of double | the upper half values for the specified field     |
 * </pre>
 * <p>
 * Note: This implementation uses 2-TreeMultiset as an alternative to the 2-PriorityQueue approach due to serialization
 * issues, while still providing an efficient median calculation in streaming environments.
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
     * finding the middle value using two TreeMultisets (lowerHalf and upperHalf).
     */
    private Median(String fieldNameToMedian) {
        super(DEFAULT_FIELD);
        this.statsFunction = (groupKey, value, aggregate) -> {
            String fieldNameLowerHalf = createInternalFieldName("lowerHalf");
            String fieldNameUpperHalf = createInternalFieldName("upperHalf");

            TreeMultiset<Double> lowerHalf = aggregate.get(fieldNameLowerHalf);
            TreeMultiset<Double> upperHalf = aggregate.get(fieldNameUpperHalf);

            if (lowerHalf == null) {
                lowerHalf = TreeMultiset.create();
            }
            if (upperHalf == null) {
                upperHalf = TreeMultiset.create();
            }

            Double fieldValue = value.getNumber(fieldNameToMedian).doubleValue();
            addValue(fieldValue, lowerHalf, upperHalf);
            rebalanceMultisets(lowerHalf, upperHalf);

            aggregate.set(fieldNameLowerHalf, lowerHalf);
            aggregate.set(fieldNameUpperHalf, upperHalf);

            return calculateMedian(lowerHalf, upperHalf);
        };
    }

    private void addValue(Double fieldValue, TreeMultiset<Double> lowerHalf, TreeMultiset<Double> upperHalf) {
        if (lowerHalf.isEmpty() || fieldValue < lowerHalf.lastEntry().getElement()) {
            lowerHalf.add(fieldValue);
        } else {
            upperHalf.add(fieldValue);
        }
    }

    private void rebalanceMultisets(TreeMultiset<Double> lowerHalf, TreeMultiset<Double> upperHalf) {
        while (lowerHalf.size() > upperHalf.size() + 1) {
            upperHalf.add(lowerHalf.pollLastEntry().getElement());
        }
        while (upperHalf.size() > lowerHalf.size()) {
            lowerHalf.add(upperHalf.pollFirstEntry().getElement());
        }
    }

    private double calculateMedian(TreeMultiset<Double> lowerHalf, TreeMultiset<Double> upperHalf) {
        if (lowerHalf.size() == upperHalf.size()) {
            return (lowerHalf.lastEntry().getElement() + upperHalf.firstEntry().getElement()) / 2;
        } else {
            return lowerHalf.lastEntry().getElement();
        }
    }
}
