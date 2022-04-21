package de.tradingpulse.streams.recordtypes;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@EqualsAndHashCode
@ToString(callSuper = true)
@NoArgsConstructor
public class TransactionRecord<GK, V> {
	
//	@SuppressWarnings("unchecked")
//	public static <GK, V extends AbstractIncrementalAggregateRecord> TransactionRecord<GK, V> createFrom(V value) {
//		return (TransactionRecord<GK, V>) TransactionRecord.builder()
//				.key(value.getKey().deepClone())
//				.timeRange(value.getTimeRange())
//				.build();
//	}
	
	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
	private LinkedList<V> records = new LinkedList<>();
	
	/**
	 * The groupKey used to create this transaction.
	 */
	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
	private GK groupKey;
	
	/**
	 * Returns the list of unique records making up this transaction. 
	 */
	public List<V> getRecords() {
		return Collections.unmodifiableList(records);
	}

	/**
	 * Returns the record at the index position in this transaction. The index
	 * can be negative to select records starting from the end of this
	 * transaction.
	 * 
	 * @param index 0-indexed index
	 * @return
	 * 	the record at the specified index position
	 * 
	 * @throws IndexOutOfBoundsException
	 */
	public V getRecord(int index) {
		int i = index >= 0 ? index : this.records.size() + index;

		if( i < 0 || i >= this.records.size()) {
			throw new IndexOutOfBoundsException(
					String.format(
							"resulting index '%d' from index parameter '%d' doesn't match list of records of size %d",
							i,
							index,
							this.records.size()));
		}
		
		return this.records.get(i);
	}
	
	/**
	 * Adds the given value to the transaction value list. <br>
	 * <br>
	 * If the value was already added, this method doesn't do anything.
	 *   
	 * @param value	the value to add
	 */
	public void addUnique(V value) {
		Objects.requireNonNull(value, "value must be not null");
		
		if(records.contains(value)) {
			return;
		}
		
		records.add(value);
//		getKey().setTimestamp(Math.max(getKey().getTimestamp(), value.getKey().getTimestamp()));
	}
}