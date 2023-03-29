package io.kipe.streams.kafka.processors.expressions.stats;

import io.kipe.streams.kafka.processors.StatsExpression;

/**
 * Computes the maximum value for a specified field within a dataset.
 *
 * <p>
 * <strong>SDK Construct</strong>: Max
 * </p>
 *
 * <p>
 * <strong>Fields</strong>:
 * </p>
 * <table>
 *   <thead>
 *     <tr>
 *       <th>Field Name</th>
 *       <th>Field Type</th>
 *       <th>Description</th>
 *       <th>Renaming Procedure</th>
 *     </tr>
 *   </thead>
 *   <tbody>
 *     <tr>
 *       <td>fieldNameToMax</td>
 *       <td>UserField</td>
 *       <td>The input field name for which to find the maximum value.</td>
 *       <td>N/A</td>
 *     </tr>
 *     <tr>
 *       <td>max</td>
 *       <td>SDKField</td>
 *       <td>The maximum value of the specified field.</td>
 *       <td>Use the {@link #max(String, String)} method to rename field.</td>
 *     </tr>
 *   </tbody>
 * </table>
 */
public class Max extends StatsExpression {

    public static final String DEFAULT_FIELD = "max";

    /**
     * Returns a new {@code Max} instance configured to compute the maximum value
     * for the specified field name.
     *
     * @param fieldNameToMax the field name for which to find the maximum value
     * @return a new {@code Max} instance
     */
    public static Max max(String fieldNameToMax) {
        return new Max(fieldNameToMax, DEFAULT_FIELD);
    }

    /**
     * Returns a new {@code Max} instance configured to compute the maximum value
     * for the specified field name and use a custom output field name.
     *
     * @param fieldNameToMax  the field name for which to find the maximum value
     * @param outputFieldName the custom field name for the output
     * @return a new {@code Max} instance
     */
    public static Max max(String fieldNameToMax, String outputFieldName) {
        return new Max(fieldNameToMax, outputFieldName);
    }

    /**
     * Private constructor for the {@code Max} class. It calls the constructor of the
     * parent class {@link StatsExpression} with the specified output field name and
     * sets the {@code statsFunction}.
     *
     * @param fieldNameToMax  the field name for which to find the maximum value
     * @param outputFieldName the field name for the output
     */
    private Max(String fieldNameToMax, String outputFieldName) {
        super(outputFieldName);
        this.statsFunction = (groupKey, value, aggregate) -> {
            var currentMax = aggregate.getNumber(this.fieldName);
            var newValue = value.getNumber(fieldNameToMax);
            return currentMax == null || newValue.doubleValue() > currentMax.doubleValue() ? newValue : currentMax;
        };
    }
}
