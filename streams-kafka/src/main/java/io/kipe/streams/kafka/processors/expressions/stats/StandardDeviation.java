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
 * The StandardDeviation class calculates the standard deviation of values in a data stream using Welford's algorithm
 * for better numerical stability.
 * <p>
 * Note that an Exception will be thrown if a null field value is encountered during processing.
 * <p>
 * The fields for this statistical expression are as follows:
 * <pre>
 * | field           | internal | type   | description                                                  |
 * |-----------------|----------|--------|--------------------------------------------------------------|
 * | stdev or stdevp | no       | double | the standard deviation of values at the measured value field |
 * | count           | yes      | long   | the number of values processed                               |
 * | mean            | yes      | double | the running mean of the values                               |
 * | ssd             | yes      | double | the running sum of squared differences from the mean         |
 * </pre>
 */
public class StandardDeviation extends StatsExpression {
    public static final String DEFAULT_SAMPLE_STDEV_FIELD = "stdev";
    public static final String DEFAULT_POPULATION_STDEV_FIELD = "stdevp";

    public enum StandardDeviationType {
        SAMPLE, POPULATION
    }

    /**
     * Returns a new StandardDeviation instance for the specified field and standard deviation type.
     *
     * @param fieldNameToStdev the field for which the standard deviation will be calculated
     * @param stdevType        the type of standard deviation to calculate (sample or population)
     * @return a new StandardDeviation instance for the given field
     * @throws StreamsException     when an error occurs during processing, such as encountering a null field value
     * @throws NullPointerException when a null field value is encountered, causing an issue in processing (underlying
     *                              cause of StreamsException)
     */
    public static StandardDeviation stdev(String fieldNameToStdev, StandardDeviationType stdevType) {
        String defaultField = stdevType == StandardDeviationType.SAMPLE ? DEFAULT_SAMPLE_STDEV_FIELD : DEFAULT_POPULATION_STDEV_FIELD;
        return new StandardDeviation(fieldNameToStdev, stdevType, defaultField);
    }

    /**
     * Returns a new StandardDeviation instance for the specified field, calculating sample standard deviation.
     *
     * @param fieldNameToStdev the field for which the standard deviation will be calculated
     * @return a new StandardDeviation instance for the given field
     */
    public static StandardDeviation stdev(String fieldNameToStdev) {
        return stdev(fieldNameToStdev, StandardDeviationType.SAMPLE);
    }

    /**
     * Returns a new StandardDeviation instance for the specified field, calculating population standard deviation.
     *
     * @param fieldNameToStdev the field for which the standard deviation will be calculated
     * @return a new StandardDeviation instance for the given field
     */
    public static StandardDeviation stdevp(String fieldNameToStdev) {
        return stdev(fieldNameToStdev, StandardDeviationType.POPULATION);
    }

    /**
     * Initializes the statsFunction to calculate the standard deviation for the specified field.
     */
    private StandardDeviation(String fieldNameToStdev, StandardDeviationType stdevType, String defaultField) {
        super(defaultField);
        this.statsFunction = (groupKey, value, aggregate) -> {
            String fieldNameCount = createInternalFieldName("count");
            String fieldNameMean = createInternalFieldName("mean");
            String fieldNameSsd = createInternalFieldName("ssd");

            Double fieldValue = value.getDouble(fieldNameToStdev);

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

            double variance = updatedSsd / (stdevType.equals(StandardDeviationType.POPULATION) ? previousCount : previousCount - 1);
            double stdev = Math.sqrt(variance);
            return previousCount <= 1 ? 0.0 : stdev;
        };
    }
}
