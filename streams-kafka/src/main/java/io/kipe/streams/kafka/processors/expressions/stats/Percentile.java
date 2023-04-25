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
 * The Percentile class calculates the exact n-th percentile of values from an event stream using the "Linear
 * Interpolation Between Closest Ranks" algorithm.
 * <p>
 * The fields for this statistical expression are as follows:
 * <pre>
 * | field      | internal | type            | description                                     |
 * |------------|----------|-----------------|-------------------------------------------------|
 * | percentile | no       | double          | the n-th percentile at the measured value field |
 * | values     | yes      | list of doubles | the values in the event stream                  |
 * </pre>
 */
public class Percentile extends StatsExpression {

    public static final String DEFAULT_FIELD = "percentile";
    private final int percentile;

    public static Percentile median(String fieldName) {
        return new Percentile(fieldName, 50);
    }

    public static Percentile perc5(String fieldName) {
        return new Percentile(fieldName, 5);
    }

    public static Percentile perc10(String fieldName) {
        return new Percentile(fieldName, 10);
    }

    public static Percentile perc15(String fieldName) {
        return new Percentile(fieldName, 15);
    }

    public static Percentile perc20(String fieldName) {
        return new Percentile(fieldName, 20);
    }

    public static Percentile perc25(String fieldName) {
        return new Percentile(fieldName, 25);
    }

    public static Percentile perc30(String fieldName) {
        return new Percentile(fieldName, 30);
    }

    public static Percentile perc35(String fieldName) {
        return new Percentile(fieldName, 35);
    }

    public static Percentile perc40(String fieldName) {
        return new Percentile(fieldName, 40);
    }

    public static Percentile perc45(String fieldName) {
        return new Percentile(fieldName, 45);
    }

    public static Percentile perc50(String fieldName) {
        return new Percentile(fieldName, 50);
    }

    public static Percentile perc55(String fieldName) {
        return new Percentile(fieldName, 55);
    }

    public static Percentile perc60(String fieldName) {
        return new Percentile(fieldName, 60);
    }

    public static Percentile perc65(String fieldName) {
        return new Percentile(fieldName, 65);
    }

    public static Percentile perc70(String fieldName) {
        return new Percentile(fieldName, 70);
    }

    public static Percentile perc75(String fieldName) {
        return new Percentile(fieldName, 75);
    }

    public static Percentile perc80(String fieldName) {
        return new Percentile(fieldName, 80);
    }

    public static Percentile perc85(String fieldName) {
        return new Percentile(fieldName, 85);
    }

    public static Percentile perc90(String fieldName) {
        return new Percentile(fieldName, 90);
    }

    public static Percentile perc95(String fieldName) {
        return new Percentile(fieldName, 95);
    }

    private Percentile(String fieldName, int percentile) {
        super(DEFAULT_FIELD);
        this.percentile = percentile;
        this.statsFunction = (groupKey, value, aggregate) -> {
            String fieldNameValues = createInternalFieldName("values");

            Double fieldValue = value.get(fieldName);

            if (fieldValue == null) {
                return aggregate.get(this.fieldName);
            }

            List<Double> values = aggregate.get(fieldNameValues, ArrayList::new);
            insertSorted(values, fieldValue);
            aggregate.set(fieldNameValues, values);

            return calculatePercentile(values, percentile);
        };
    }

    private void insertSorted(List<Double> values, double fieldValue) {
        int index = Collections.binarySearch(values, fieldValue);
        if (index < 0) {
            index = -(index + 1);
        }
        values.add(index, fieldValue);
    }

    private double calculatePercentile(List<Double> values, int percentile) {
        int numValues = values.size();

        if (numValues == 1) {
            return values.get(0);
        }

        double rank = percentile / 100.0 * (numValues - 1) + 1.0;
        int lowerIndex = (int) Math.floor(rank);

        double weight = rank - lowerIndex;
        double lowerValue = values.get(lowerIndex - 1);
        double upperValue = values.get(lowerIndex);

        return lowerValue + weight * (upperValue - lowerValue);
    }

}
