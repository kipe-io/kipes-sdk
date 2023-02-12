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
	 * The update method is used to update the specified field in the
	 * {@link GenericRecord} object with the new value returned by the valueFunction
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