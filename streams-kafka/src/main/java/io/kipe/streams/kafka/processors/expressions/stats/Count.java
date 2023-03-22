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
 * Stats expression to count records.
 * <p>
 * This class provides a singleton instance of the Count expression and a default field name "count"
 * which will be used to store the count value in the resulting record.
 * <p>
 * The valueFunction provided in the constructor takes in a key and value and returns the count value
 * by first attempting to retrieve the count value from the provided field name in the value,
 * and if it is not present, it defaults to 1.
 * <p>
 * The class also provides a static factory method count() to retrieve the singleton instance.
 */
public class Count extends StatsExpression {

	public static final String DEFAULT_FIELD = "count";
	private static final Count SINGLETON = new Count();

	/**
	 * Returns the singleton instance of this class
	 *
	 * @return Count singleton instance
	 */
	public static Count count() {
		return SINGLETON;
	}

	/**
	 * Constructor for Count class, which calls the constructor of the parent class {@link StatsExpression}
	 * with the default field name "count". It also sets the valueFunction in the constructor.
	 */
	private Count() {
		super(DEFAULT_FIELD);
		this.valueFunction = (key, value) -> {
			Number count = value.getNumber(this.fieldName);
			return count == null? 1L : count.longValue() + 1L;
		};
	}
}
