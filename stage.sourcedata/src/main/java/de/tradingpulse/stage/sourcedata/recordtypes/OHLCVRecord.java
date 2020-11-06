package de.tradingpulse.stage.sourcedata.recordtypes;

import java.time.LocalDate;
import java.time.ZoneOffset;

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TimeRange;
import de.tradingpulse.common.utils.TimeUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class OHLCVRecord extends AbstractIncrementalAggregateRecord {
	
	public static final OHLCVRecord from(OHLCVRawRecord rawData) {
		return builder()
				.key(extractKey(rawData))
				.timeRange(TimeRange.DAY)
				.open(rawData.getOpen())
				.high(rawData.getHigh())
				.low(rawData.getLow())
				.close(rawData.getClose())
				.volume(rawData.getVolume())
				.build();
	}
	
	static final SymbolTimestampKey extractKey(OHLCVRawRecord rawData) {
		
		return SymbolTimestampKey.builder()
				.symbol(rawData.getSymbol())
				.timestamp(
						LocalDate.parse(rawData.getDate(), TimeUtils.FORMATTER_YYYY_MM_DD)
						.atStartOfDay()
						.toEpochSecond(ZoneOffset.UTC)
						* 1000)
				.build();
	}
	
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Long volume;
    
    /**
     * Aggregates this object with the other and returns a new OHLCVData with
     * the same key as this object and adjusted ohlcv data matching an
     * time aggregate of both objects.
     */
    public OHLCVRecord aggregateWith(OHLCVRecord other) {
    	
    	boolean isOtherLater = getKey().getTimestamp() < other.getKey().getTimestamp();
    	
    	return OHLCVRecord.builder()
    			.key(SymbolTimestampKey.builder()
    					.symbol(getKey().getSymbol())
    					.timestamp(isOtherLater? other.getKey().getTimestamp() : getKey().getTimestamp())
    					.build())
    			.timeRange(getTimeRange())
    			.open(isOtherLater? this.open : other.open)
    			.high(this.high.doubleValue() > other.high.doubleValue()? this.high : other.high)
    			.low(this.low.doubleValue() < other.low.doubleValue()? this.low : other.low)
    			.close(isOtherLater? other.close : this.close)
    			.volume(this.volume.longValue() + other.volume.longValue())
    			.build();
    }
}
