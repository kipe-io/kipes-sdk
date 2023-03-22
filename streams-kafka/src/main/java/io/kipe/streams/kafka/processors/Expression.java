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

import java.util.function.BiFunction;

import io.kipe.streams.recordtypes.GenericRecord;
import lombok.AllArgsConstructor;
import lombok.Setter;

/**
 * Simple construct to update a {@link GenericRecord} field from a function.
 *
 * @param <K> the key type
 * @param <V> the GenericRecord
 */
@AllArgsConstructor 
public class Expression<K, V extends GenericRecord> {
	
	@Setter
	protected String fieldName;
	protected BiFunction<K, V, Object> valueFunction;
	
	protected Expression() {}

	/**
	 * The update method is used to update the specified field in the {@link GenericRecord} object with the new value
	 * returned by the valueFunction
	 *
	 * @param key    the key of the record
	 * @param record the GenericRecord object to update
	 **/
	void update(K key, V record) {
		record.set(
				this.fieldName, 
				this.valueFunction.apply(key, record));
	}
}