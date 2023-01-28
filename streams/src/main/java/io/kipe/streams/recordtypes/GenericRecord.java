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

	/**
	 * Creates a new instance of the {@link GenericRecord} class.
	 *
	 * @return a new instance of the {@link GenericRecord} class.
	 */
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
	 * Adds the fields from the other {@link GenericRecord} if those fields unknown at
	 * this object.
	 *
	 * @param other the {@link GenericRecord} to add the unknown fields from.
	 * @return this object.
	 */
	public GenericRecord withNewFieldsFrom(GenericRecord other) {
		Objects.requireNonNull(other, "other");
		
		other.fields.forEach(this.fields::putIfAbsent);
		
		return this;
	}

	/**
	 * Copies the value of a field from another {@link GenericRecord} object and sets it in this object.
	 *
	 * @param fieldName The name of the field to be copied.
	 * @param other     The other {@link GenericRecord} object from which the field value will be copied.
	 * @return this object.
	 */
	public GenericRecord withValueFrom(String fieldName, GenericRecord other) {
		set(fieldName, other.get(fieldName));
		return this;
	}

	/**
	 * Returns the value of the given field.
	 *
	 * @param fieldName the field's name to return the value for.
	 * @return the current field's value or {@code null} if there is no such field.
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
	 * @param <V>       the field's new value type.
	 * @param fieldName the field to set.
	 * @param value     the field's new value.
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
	 * @param <V> TODO
	 * @param fieldName the field to set.
	 * @param value the field's new value.
	 * @return this object.
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

	/**
	 * Retrieves the value of the specified field as a String.
	 *
	 * @param fieldName the name of the field to retrieve the value for.
	 * @return the value of the field as a String, or null if the field is not present or has a null value.
	 */
	public String getString(String fieldName) {
		Object o = get(fieldName);
		return o == null? null : o.toString();
	}

	/**
	 * Retrieves the value of the specified field as a Number.
	 *
	 * @param fieldName the name of the field to retrieve the value for.
	 * @return the value of the field as a Number, or null if the field is not present or has a null value.
	 */
	public Number getNumber(String fieldName) {
		Object o = get(fieldName);
		return o == null? null : (Number)o;
	}

	/**
	 * Retrieves the value of the specified field as a Double.
	 *
	 * @param fieldName the name of the field to retrieve the value for.
	 * @return the value of the field as a Double, or null if the field is not present or has a null value.
	 */
	public Double getDouble(String fieldName) {
		Number n = getNumber(fieldName);
		return n == null? null : n.doubleValue();
	}

	/**
	 * Retrieves the value of the specified field as a Map.
	 *
	 * @param fieldName the name of the field to retrieve the value for.
	 * @return the value of the field as a Map, or null if the field is not present or has a null value.
	 */
	@SuppressWarnings("unchecked")
	public <K,V> Map<K,V> getMap(String fieldName) {
		return (Map<K,V>) get(fieldName);
	}
}
