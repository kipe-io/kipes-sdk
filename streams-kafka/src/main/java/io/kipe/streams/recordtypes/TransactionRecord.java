/*
 * Kipe Streams Kafka - Kipe Streams SDK
 * Copyright © 2023 Kipe.io
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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * TransactionRecord is a class representing a transaction made up of unique records.
 * <p>
 * This class can be used to store a set of unique records that belong to the same transaction.
 * The records stored in this class should have a unique key, as the class uses the equals method to determine
 * whether a record was already added or not.
 *
 * @param <GK> type of the groupKey used to create this transaction.
 * @param <V>  type of the records making up this transaction.
 */
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

	/**
	 * List of unique records making up this transaction.
	 */
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
	 * @param index 0-indexed index.
	 * @return the record at the specified index position.
	 * @throws IndexOutOfBoundsException if the resulting index is out of bounds.
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
	 * @param value the value to add.
	 * @throws NullPointerException if value is null.
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