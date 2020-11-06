package de.tradingpulse.connector.iexcloud;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import de.tradingpulse.common.utils.TimeUtils;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
class SymbolOffset implements Comparable<SymbolOffset> {
	
	// ------------------------------------------------------------------------
	// static
	// ------------------------------------------------------------------------

	private static final String PARTITION_KEY_SYMBOL = "symbol";
	private static final String OFFSET_KEY_LAST_FETCHED_DATE = "lastFetchedDate";

	static SymbolOffset fromKafkaPartitionAndOffset(
			Map<String, String> partition,
			Map<String, Object> offset)
	{
		return new SymbolOffset(
				partition.get(PARTITION_KEY_SYMBOL), 
				LocalDate.parse((String)offset.get(OFFSET_KEY_LAST_FETCHED_DATE), TimeUtils.FORMATTER_YYYY_MM_DD));
	}
	
	static Map<String, String> createKafkaPartition(String symbol) {
		Map<String, String> partitionMap = new HashMap<>();
		partitionMap.put(PARTITION_KEY_SYMBOL, symbol);
		return partitionMap;
	}
	
	static Map<String, Object> createKafkaOffset(LocalDate lastFetchedDate) {
		Map<String, Object> offsetMap = new HashMap<>();
		offsetMap.put(OFFSET_KEY_LAST_FETCHED_DATE, lastFetchedDate.format(TimeUtils.FORMATTER_YYYY_MM_DD));
		return offsetMap;
	}
	
	static int compare(LocalDate lastFetchedDate, LocalDate otherLastFetchedDate) {
		if(lastFetchedDate == null) {
			if(otherLastFetchedDate == null) {
				return 0;
			}
			// this comes last
			return 1;
		}
		
		if(otherLastFetchedDate == null) {
			// other comes last
			return -1;
		}
		
		return -1 * (int)Math.signum(lastFetchedDate.compareTo(otherLastFetchedDate));			
	}
	
	// ------------------------------------------------------------------------
	// instance
	// ------------------------------------------------------------------------

	final String symbol;
	final LocalDate lastFetchedDate;
	
	SymbolOffset(String symbol, LocalDate lastFetchedDate){
		this.symbol = symbol;
		this.lastFetchedDate = lastFetchedDate;
	}
	
	@Override
	public int compareTo(SymbolOffset o) {
		int lastFetchDateCompare = compare(this.lastFetchedDate, o == null? null : o.lastFetchedDate);
		if(lastFetchDateCompare != 0) {
			return lastFetchDateCompare;
		}
		return this.symbol.compareTo(o.symbol);
	}
	
	Map<String, String> asKafkaConnectPartition() {
		return createKafkaPartition(this.symbol);
	}
	
	Map<String, Object> asKafkaConnectOffset() {
		return createKafkaOffset(this.lastFetchedDate);
	}
}