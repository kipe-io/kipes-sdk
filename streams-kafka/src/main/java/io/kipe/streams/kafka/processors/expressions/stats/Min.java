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

/**
 * Computes the minimum value for a specified field within a dataset.
 *
 * <p>
 * <strong>SDK Construct</strong>: Min
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
 *       <td>fieldNameToMin</td>
 *       <td>UserField</td>
 *       <td>The input field name for which to find the minimum value.</td>
 *       <td>N/A</td>
 *     </tr>
 *     <tr>
 *       <td>min</td>
 *       <td>SDKField</td>
 *       <td>The minimum value of the specified field.</td>
 *       <td>Use the {@link #min(String, String)} method to rename field.</td>
 *     </tr>
 *   </tbody>
 * </table>
 */
public class Min extends StatsExpression {

    public static final String DEFAULT_FIELD = "min";

    /**
     * Returns a new {@code Min} instance configured to compute the minimum value
     * for the specified field name.
     *
     * @param fieldNameToMin the field name for which to find the minimum value
     * @return a new {@code Min} instance
     */
    public static Min min(String fieldNameToMin) {
        return new Min(fieldNameToMin, DEFAULT_FIELD);
    }

    /**
     * Returns a new {@code Min} instance configured to compute the minimum value
     * for the specified field name and use a custom output field name.
     *
     * @param fieldNameToMin  the field name for which to find the minimum value
     * @param outputFieldName the custom field name for the output
     * @return a new {@code Min} instance
     */
    public static Min min(String fieldNameToMin, String outputFieldName) {
        return new Min(fieldNameToMin, outputFieldName);
    }

    /**
     * Private constructor for the {@code Min} class. It calls the constructor of the
     * parent class {@link StatsExpression} with the specified output field name and
     * sets the {@code statsFunction}.
     *
     * @param fieldNameToMin  the field name for which to find the minimum value
     * @param outputFieldName the field name for the output
     */
    private Min(String fieldNameToMin, String outputFieldName) {
        super(outputFieldName);
        this.statsFunction = (groupKey, value, aggregate) -> {
            var currentMin = aggregate.getNumber(this.fieldName);
            var newValue = value.getNumber(fieldNameToMin);
            return currentMin == null || newValue.doubleValue() < currentMin.doubleValue() ? newValue : currentMin;
        };
    }
}
