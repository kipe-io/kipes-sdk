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
package io.kipe.streams.recordtypes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class GenericRecord {
	public static GenericRecord create() {
		return new GenericRecord();
	}
	
	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
	private Map<String, Object> fields = new HashMap<>();
	public GenericRecord copy() {
		GenericRecord copy = new GenericRecord();
		copy.fields.putAll(this.fields);
		
		return this;
	}

	public GenericRecord withNewValues(Map<String, Object> values) {
		Objects.requireNonNull(values, "values");
		this.fields.putAll(values);
		return this;
	}

	public GenericRecord withNewFieldsFrom(GenericRecord other) {
		Objects.requireNonNull(other, "other");
		
		other.fields.forEach(this.fields::putIfAbsent);
		
		return this;
	}
	public GenericRecord withValueFrom(String fieldName, GenericRecord other) {
		set(fieldName, other.get(fieldName));
		return this;
	}
	@SuppressWarnings("unchecked")
	public <V> V get(String fieldName) {
		Objects.requireNonNull(fieldName, "fieldName");
		return (V)fields.get(fieldName);
	}
	public <V> void set(String fieldName, V value) {
		Objects.requireNonNull(fieldName, "fieldName");
		
		if(value == null) {
			remove(fieldName);
		}
		
		this.fields.put(fieldName, value);
	}
	public <V> GenericRecord with(String fieldName, V value) {
		set(fieldName, value);
		return this;
	}
	public void remove(String fieldName) {
		Objects.requireNonNull(fieldName, "fieldName");
		this.fields.remove(fieldName);
	}
	
	// ------------------------------------------------------------------------
	// specialized getters
	// ------------------------------------------------------------------------
	public String getString(String fieldName) {
		Object o = get(fieldName);
		return o == null? null : o.toString();
	}
	public Number getNumber(String fieldName) {
		Object o = get(fieldName);
		return o == null? null : (Number)o;
	}
	public Double getDouble(String fieldName) {
		Number n = getNumber(fieldName);
		return n == null? null : n.doubleValue();
	}
	@SuppressWarnings("unchecked")
	public <K,V> Map<K,V> getMap(String fieldName) {
		return (Map<K,V>) get(fieldName);
	}
}
