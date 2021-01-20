package de.tradingpulse.common.stream.recordtypes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * A record of dynamic fields.
 */
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class GenericRecord {

	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
	private Map<String, Object> fields = new HashMap<>();
	
	/**
	 * Returns the value of the given field.
	 * 
	 * @param fieldName the field's name to return the value for
	 * 
	 * @return
	 * 	the current field's value or {@code null} if there is no such field.
	 */
	@SuppressWarnings("unchecked")
	public <V> V get(String fieldName) {
		Objects.requireNonNull(fieldName, "fieldName");
		return (V)fields.get(fieldName);
	}
	
	/**
	 * Sets a field. If the value is {@code null} the field will be removed.<br>
	 * <br>
	 * All values added to a GenericRecord need to be de/serializable from/to
	 * json by Jackson.
	 * 
	 * @param <V> the field's new value type
	 * 
	 * @param fieldName	the field to set 
	 * @param value the field's new value
	 */
	public <V> void set(String fieldName, V value) {
		Objects.requireNonNull(fieldName, "fieldName");
		
		if(value == null) {
			remove(fieldName);
		}
		
		this.fields.put(fieldName, value);
	}
	
	/**
	 * Removes the given field.
	 * 
	 * @param fieldName the field to be removed.
	 */
	public void remove(String fieldName) {
		Objects.requireNonNull(fieldName, "fieldName");
		this.fields.remove(fieldName);
	}
}
