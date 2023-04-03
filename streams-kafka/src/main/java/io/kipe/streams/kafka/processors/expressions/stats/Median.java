package io.kipe.streams.kafka.processors.expressions.stats;

import io.kipe.streams.kafka.processors.StatsExpression;

import java.util.Comparator;
import java.util.PriorityQueue;

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
     * finding the middle value using two heaps.
     */
    private Median(String fieldNameToMedian) {
        super(DEFAULT_FIELD);
        this.statsFunction = (groupKey, value, aggregate) -> {
            String fieldNameLowerHalf = createInternalFieldName("lowerHalf");
            String fieldNameUpperHalf = createInternalFieldName("upperHalf");

            PriorityQueue<Double> lowerHalf = aggregate.get(fieldNameLowerHalf);
            PriorityQueue<Double> upperHalf = aggregate.get(fieldNameUpperHalf);

            if (lowerHalf == null) {
                lowerHalf = new PriorityQueue<>(Comparator.reverseOrder());
            }
            if (upperHalf == null) {
                upperHalf = new PriorityQueue<>(Comparator.naturalOrder());
            }

            Double fieldValue = value.getNumber(fieldNameToMedian).doubleValue();

            if (lowerHalf.isEmpty() || fieldValue < lowerHalf.peek()) {
                lowerHalf.add(fieldValue);
            } else {
                upperHalf.add(fieldValue);
            }

            // Rebalance the heaps
            while (lowerHalf.size() > upperHalf.size() + 1) {
                upperHalf.add(lowerHalf.poll());
            }
            while (upperHalf.size() > lowerHalf.size()) {
                lowerHalf.add(upperHalf.poll());
            }

            aggregate.set(fieldNameLowerHalf, lowerHalf);
            aggregate.set(fieldNameUpperHalf, upperHalf);

            if (lowerHalf.size() == upperHalf.size()) {
                return (lowerHalf.peek() + upperHalf.peek()) / 2;
            } else {
                return lowerHalf.peek();
            }
        };
    }
}
