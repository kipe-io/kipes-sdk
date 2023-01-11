package io.kipe.streams.recordtypes;

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

	public static GenericRecord create() {
		return new GenericRecord();
	}
	
	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
	private Map<String, Object> fields = new HashMap<>();
	
	/**
	 * Returns a shallow copy.
	 * 
	 * @return
	 * 	a shallow copy.
	 */
	public GenericRecord copy() {
		GenericRecord copy = new GenericRecord();
		copy.fields.putAll(this.fields);
		
		return this;
	}
	
	/**
	 * Adds the fields from the other GenericRecord if those fields unknown at
	 * this object.
	 * 
	 * @param other the GenericObject to add the unknown fields from
	 * 
	 * @return
	 * 	this object
	 */
	public GenericRecord withNewFieldsFrom(GenericRecord other) {
		Objects.requireNonNull(other, "other");
		
		other.fields.forEach(this.fields::putIfAbsent);
		
		return this;
	}
	
	public GenericRecord withValueFrom(String fieldName, GenericRecord other) {
		set(fieldName, other.get(fieldName));
		return this;
	}
		
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
	 * Fluent variant of {@link #set(String, Object)}.
	 * 
	 * @param <V>
	 * @param fieldName
	 * @param value
	 * 
	 * @return
	 * 	this object
	 */
	public <V> GenericRecord with(String fieldName, V value) {
		set(fieldName, value);
		return this;
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
