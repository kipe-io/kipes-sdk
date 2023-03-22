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
public class Count extends StatsExpression {

	public static final String DEFAULT_FIELD = "count";
	private static final Count SINGLETON = new Count();
	public static Count count() {
		return SINGLETON;
	}
	private Count() {
		super(DEFAULT_FIELD);
		this.valueFunction = (key, value) -> {
			Number count = value.getNumber(this.fieldName);
			return count == null? 1L : count.longValue() + 1L;
		};
	}
}
