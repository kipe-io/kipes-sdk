package de.tradingpulse.common.stream.aggregates;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Aggregates aggregating values of time series. Whenever a new value arrives
 * that value changes the aggregate. However, that behavior is not desired when
 * handling incremental time series. An incremental time series contains
 * multiple values for the same time stamp. The last seen value is always the 
 * current one, overwriting all former ones.
 * 
 * Imagine now for instance an aggregate which counts the days seen in a time
 * series with hourly records. This is where the IncrementalAggregate can
 * help: Based on the time stamp of the event - normalized to days - you get the
 * stableAggregate of the former day.   
 * 
 * Example:
 * <pre>
 * | rec # | orig ts          | norm ts          | getAggregate(norm_ts) | setAggregate(norm_ts, X) |
 * +-------+------------------+------------------+-----------------------+--------------------------+
 * | 1     | 1970-01-01 01:00 | 1970-01-01 00:00 | null                  | 1                        |
 * | 2     | 1970-01-01 02:00 | 1970-01-01 00:00 | null                  | 1                        |
 * | 3     | 1970-01-01 03:00 | 1970-01-01 00:00 | null                  | 1                        |
 * | 4     | 1970-01-02 01:00 | 1970-01-02 00:00 | 1                     | 2                        |
 * | 5     | 1970-01-02 05:00 | 1970-01-02 00:00 | 1                     | 2                        |
 * | 6     | 1970-01-03 02:00 | 1970-01-03 00:00 | 2                     | 3                        |
 * | 7     | 1970-01-04 01:00 | 1970-01-04 00:00 | 3                     | 4                        |
 * </pre>
 * 
 * Please note that only two consecutive time stamps T1 and T2 are being
 * managed. As soon as you introduce a new timestamp T3 via 
 * {@link #setAggregate(long, Object)} AND T2 < T3, T1 gets evicted. All other
 * time stamps will be ignored. 
 * 
 * Example:
 * <pre>
 * | rec # | setAggregate(TS, ...) | T1 | T2 |
 * +-------+-----------------------+----+----+
 * | 1     | 1                     | -  | 1  |
 * | 2     | 0                     | -  | 1  |
 * | 3     | 1                     | -  | 1  |
 * | 4     | 3                     | 1  | 3  |
 * | 5     | 2                     | 1  | 3  |
 * | 6     | 4                     | 3  | 4  |
 * </pre>
 * 
 * @param <A>
 * The type of the managed incremental aggregate pair.
 */
@Data
@NoArgsConstructor
public class IncrementalAggregate <A extends DeepCloneable<A>> {
	
	private Long currentTimestamp;
	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
	private A currentAggregate;

	private Long stableTimestamp;
	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
	private A stableAggregate;
	
	public A getAggregate(final long timestamp) {
		
		if(this.currentTimestamp == null) {
			// client hasn't stored anything yet
			return null;
		}
		
		if(this.currentTimestamp.longValue() < timestamp) {
			// client asks for a later timestamp, so current is his base
			return currentAggregate.deepClone();
		}
		
		if(this.currentTimestamp.longValue() == timestamp) {
			// client asks for same timestamp, so stable is his base
			return stableAggregate == null? null : stableAggregate.deepClone();
		}
		
		if(this.stableTimestamp == null) {
			// client wants a ts < current but we haven't seen anything else
			// yet
			return null;
		}
		
		if(this.stableTimestamp.longValue() < timestamp) {
			// client wants a ts between stable and current, seems like the
			// client gets values out-of-order. 
			// Still let's give him stable. The client might be able to handle
			// that.
		
			return stableAggregate.deepClone();
		}
		
		// for timestamp <= stable we can't return anything
		
		return null;
	}
	
	/**
	 * Registers a deep clone of the aggregate with the given timestamp.
	 * 
	 * @return whether or not the aggregate was in the right order and had been stored
	 */
	public boolean setAggregate(final long timestamp, final A aggregate) {
		
		if(this.currentTimestamp == null) {
			// nothing stored yet
			this.currentTimestamp = timestamp;
			this.currentAggregate = aggregate.deepClone();
			return true;
		}
		
		if(this.currentTimestamp.longValue() > timestamp) {
			// we have stored an later time stamp stable value, seems like the
			// client gets values out-of-order. We ignore that.
			
			return false;
		}
		
		if(this.currentTimestamp.longValue() == timestamp) {
			// client asks for the same time range so we overwrite
			this.currentAggregate = aggregate.deepClone();
			return true;
		}
		
		// later timestamp makes us shift
		this.stableTimestamp = this.currentTimestamp;
		this.stableAggregate = this.currentAggregate.deepClone();
		this.currentTimestamp = timestamp;
		this.currentAggregate = aggregate.deepClone();
		
		return true;
	}
}
