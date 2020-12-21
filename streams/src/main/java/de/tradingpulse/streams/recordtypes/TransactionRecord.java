package de.tradingpulse.streams.recordtypes;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class TransactionRecord<V extends AbstractIncrementalAggregateRecord> extends AbstractIncrementalAggregateRecord {
	
	@SuppressWarnings("unchecked")
	public static <V extends AbstractIncrementalAggregateRecord> TransactionRecord<V> createFrom(V value) {
		TransactionRecord<V> record = (TransactionRecord<V>) TransactionRecord.builder()
				.key(value.getKey().deepClone())
				.build();
		
		record.addUnique(value);
		return record;
	}
	
	@Default
	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
	private LinkedList<V> values = new LinkedList<>();
	
	public List<V> getValues() {
		return Collections.unmodifiableList(values);
	}

	/**
	 * Adds the given value to the transaction value list and adjusts 
	 * this.key.timestamp to the larger value of this.key.timestamp and
	 * value.key.timestamp. <br>
	 * <br>
	 * If the value was already added, this method doesn't do anything.
	 *   
	 * @param value	the value to add
	 */
	public void addUnique(V value) {
		Objects.requireNonNull(getKey(), "this.key must be set before");
		Objects.requireNonNull(value, "value must be not null");
		Objects.requireNonNull(value.getKey(), "value.key must be not null");
		
		if(values.contains(value)) {
			return;
		}
		
		values.add(value);
		getKey().setTimestamp(Math.max(getKey().getTimestamp(), value.getKey().getTimestamp()));
	}
}