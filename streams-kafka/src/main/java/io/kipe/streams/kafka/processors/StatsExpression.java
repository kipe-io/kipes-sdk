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

/**
 * An Abstract class for defining statistics expressions to be applied to Kafka records.
 */
public abstract class StatsExpression extends Expression<String, GenericRecord> {

	/**
	 * Constructor for creating a {@link StatsExpression}.
	 *
	 * @param defaultFieldName The default field name to be used by the
	 *                         expression.
	 */
	protected StatsExpression(String defaultFieldName) {
		this.fieldName = defaultFieldName;
	}
}
