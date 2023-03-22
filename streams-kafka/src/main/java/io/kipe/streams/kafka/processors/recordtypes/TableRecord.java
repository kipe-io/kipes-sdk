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
package io.kipe.streams.kafka.processors.recordtypes;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * A TableRecord represents a collection of key-value pairs stored in a table format. The values are stored in the form
 * of Row instances which are accessible by their keys.
 *
 * @param <K> the type of keys in the table.
 * @param <V> the type of values in the table.
 */
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class TableRecord <K,V>{

	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
	private Map<K,Row<K,V>> rows = new HashMap<>();

	/**
	 * Adds a new key-value pair to the table.
	 *
	 * @param key   the key for the new pair.
	 * @param value the value for the new pair.
	 */
	public void put(K key, V value) {
		rows.put(key, new Row<>(key, value));
	}

	/**
	 * A Row represents a single key-value pair in the table.
	 *
	 * @param <K> the type of the key in the row.
	 * @param <V> the type of the value in the row.
	 */
	@NoArgsConstructor
	@AllArgsConstructor
	@Getter
	public static class Row<K,V> {
		
		@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
		private K key;
		@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
		private V value;
	}
}
