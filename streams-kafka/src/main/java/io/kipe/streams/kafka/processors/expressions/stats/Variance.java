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
import org.apache.kafka.streams.errors.StreamsException;

/**
 * The Variance class calculates the variance of values in a data stream using Welford's algorithm for better numerical
 * stability.
 * <p>
 * The fields for this statistical expression are as follows:
 * <pre>
 * | field       | internal | type   | description                                          |
 * |-------------|----------|--------|------------------------------------------------------|
 * | var or varp | no       | double | the variance of values at the measured value field   |
 * | count       | yes      | long   | the number of values processed                       |
 * | mean        | yes      | double | the running mean of the values                       |
 * | ssd         | yes      | double | the running sum of squared differences from the mean |
 * </pre>
 */
public class Variance extends StatsExpression {
    public static final String DEFAULT_SAMPLE_VARIANCE_FIELD = "var";
    public static final String DEFAULT_POPULATION_VARIANCE_FIELD = "varp";

    public enum VarianceType {
        SAMPLE, POPULATION
    }

    /**
     * Returns a new Variance instance for the specified field and variance type.
     *
     * @param fieldNameToVariance the field for which the variance will be calculated
     * @param varianceType        the type of variance to calculate (sample or population)
     * @return a new Variance instance for the given field
     */
    public static Variance var(String fieldNameToVariance, VarianceType varianceType) {
        String defaultField = varianceType == VarianceType.SAMPLE ? DEFAULT_SAMPLE_VARIANCE_FIELD : DEFAULT_POPULATION_VARIANCE_FIELD;
        return new Variance(fieldNameToVariance, varianceType, defaultField);
    }

    /**
     * Returns a new Variance instance for the specified field, calculating sample variance.
     *
     * @param fieldNameToVariance the field for which the variance will be calculated
     * @return a new Variance instance for the given field
     */
    public static Variance var(String fieldNameToVariance) {
        return var(fieldNameToVariance, VarianceType.SAMPLE);
    }

    /**
     * Returns a new Variance instance for the specified field, calculating population variance.
     *
     * @param fieldNameToVariance the field for which the variance will be calculated
     * @return a new Variance instance for the given field
     */
    public static Variance varp(String fieldNameToVariance) {
        return var(fieldNameToVariance, VarianceType.POPULATION);
    }

    /**
     * Initializes the statsFunction to calculate the variance for the specified field.
     */
    private Variance(String fieldNameToVariance, VarianceType varianceType, String defaultField) {
        super(defaultField);
        this.statsFunction = (groupKey, value, aggregate) -> {
            String fieldNameCount = createInternalFieldName("count");
            String fieldNameMean = createInternalFieldName("mean");
            String fieldNameSsd = createInternalFieldName("ssd");

            Double fieldValue = value.getDouble(fieldNameToVariance);

            if (fieldValue == null) {
                return aggregate.getDouble(this.fieldName);
            }

            double previousMean = aggregate.get(fieldNameMean, () -> 0.0);
            long previousCount = aggregate.get(fieldNameCount, () -> 0L);
            previousCount += 1;
            double updatedMean = previousMean + (fieldValue - previousMean) / previousCount;
            double previousSsd = aggregate.get(fieldNameSsd, () -> 0.0);
            double updatedSsd = previousSsd + (fieldValue - previousMean) * (fieldValue - updatedMean);

            aggregate.set(fieldNameCount, previousCount);
            aggregate.set(fieldNameMean, updatedMean);
            aggregate.set(fieldNameSsd, updatedSsd);

            double variance = updatedSsd / (varianceType.equals(VarianceType.POPULATION) ? previousCount : previousCount - 1);
            return previousCount <= 1 ? 0.0 : variance;
        };
    }
}
