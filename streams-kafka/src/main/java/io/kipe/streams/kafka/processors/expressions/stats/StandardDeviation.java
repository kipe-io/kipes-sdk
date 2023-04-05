package io.kipe.streams.kafka.processors.expressions.stats;

import io.kipe.streams.kafka.processors.StatsExpression;
import org.apache.kafka.streams.errors.StreamsException;

/**
 * The StandardDeviation class calculates the standard deviation of values in a data stream using Welford's algorithm
 * for better numerical stability.
 * <p>
 * Note that an Exception will be thrown if a null field value is encountered during processing.
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

            double previousMean = aggregate.getDouble(fieldNameMean) == null ? 0.0 : aggregate.getDouble(fieldNameMean);
            double previousCount = aggregate.getNumber(fieldNameCount) == null ? 0.0 : aggregate.getNumber(fieldNameCount).doubleValue();
            double fieldValue = value.getDouble(fieldNameToStdev);
            previousCount += 1;
            double updatedMean = previousMean + (fieldValue - previousMean) / previousCount;
            double previousSsd = aggregate.getDouble(fieldNameSsd) == null ? 0.0 : aggregate.getDouble(fieldNameSsd);
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
