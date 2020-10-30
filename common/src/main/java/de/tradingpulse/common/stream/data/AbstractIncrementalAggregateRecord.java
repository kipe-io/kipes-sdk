package de.tradingpulse.common.stream.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

import lombok.Data;

/**
 * Foundation for all records describing financial time-line data.<br>
 * <br>
 * The core idea is that all financial records are incrementally aggregate from
 * base tick records, eventually. A tick record is would describe an atomic
 * change to a financial asset.<br>
 * <br>
 * In other words, an incremental aggregate record describes the current 
 * aggregation value at a specific time range based on the values of a lower 
 * aggregation line, or, at the most basic level, an aggregation of
 * tick values.<br>
 * <br>
 * As such, an incremental aggregate record is described by two timestamps:
 * <ul>
 *  <li>the timestamp where the change happened</li>
 *  <li>the timestamp of the aggregation time-line</li>
 * </ul>
 * The first one is part of the key ({@link #getKey()}), the second is part of
 * the record data ({@link #getTimeRangeTimestamp()}). The time range itself is
 * described by {@link #timeRange}
 */
@Data
public abstract class AbstractIncrementalAggregateRecord {

	private SymbolTimestampKey key;
	private TimeRange timeRange = TimeRange.MILLISECOND;

	@JsonProperty(access = Access.READ_ONLY)
	public long getTimeRangeTimestamp() {
		if(key == null) {
			throw new IllegalStateException("key is not specified");
		}

		if(timeRange == null) {
			throw new IllegalStateException("timeRange is not specified");
		}
		
		return timeRange.getStartOfTimeRangeFor(key.getTimestamp());
	}

}
