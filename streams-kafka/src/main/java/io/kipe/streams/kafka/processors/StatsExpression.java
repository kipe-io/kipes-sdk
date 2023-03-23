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
package io.kipe.streams.kafka.processors;

import io.kipe.streams.recordtypes.GenericRecord;
import lombok.Setter;

/**
 * An Abstract class for defining statistics expressions to be applied to Kafka records.
 */
public abstract class StatsExpression {

	@Setter
	protected String fieldName;
	protected StatsFunction<Object> statsFunction;
	
	/**
	 * Constructor for creating a {@link StatsExpression}.
	 *
	 * @param defaultFieldName The default field name to be used by the
	 *                         expression.
	 */
	protected StatsExpression(String defaultFieldName) {
		this.fieldName = defaultFieldName;
	}
	
	/**
	 * The update method is used to update the specified field in the aggregate object with the new value
	 * returned by the {@link #statsFunction}.
	 *
	 * @param groupKey  the key of the current stats group
	 * @param value     the GenericRecord to be used for calculating the statistics. 
	 * @param aggregate the GenericRecord to store the aggregated values. This object is shared between all StatsExpressions.
	 **/
	protected void update(String groupKey, GenericRecord value, GenericRecord aggregate) {
		aggregate.set(
				this.fieldName, 
				this.statsFunction.apply(groupKey, value, aggregate));
	}
	
	
	/**
	 * Functional Interface for the aggregation function of all StatsExpression to aggregate values.
	 *
	 * @param <R> the result value of the aggregation
	 */
	@FunctionalInterface
	public interface StatsFunction<R> {
		
		/**
		 * The methods calculates the result of a statistical evaluation by applying the the contents of the value to
		 * the existing contents in the aggregate.
		 * <p>
		 * Note: both the value and the aggregate objects must not be changed.
		 *   
		 * @param groupKey  the key of the current stats group 
		 * @param value     the GenericRecord that can be used to calculate the statistics
		 * @param aggregate the GenericRecord to retrieve the current, potentially not initialized aggregation status 
		 * @return
		 */
		R apply(String groupKey, GenericRecord value, GenericRecord aggregate);
	}
}
