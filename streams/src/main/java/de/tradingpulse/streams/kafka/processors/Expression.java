package de.tradingpulse.streams.kafka.processors;

import java.util.function.BiFunction;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;
import lombok.AllArgsConstructor;
import lombok.Setter;

/**
 * Simple construct to update a GenericRecord field from a function.
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
	
	void update(K key, V record) {
		record.set(
				this.fieldName, 
				this.valueFunction.apply(key, record));
	}
}