package de.tradingpulse.common.stream.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OHLCVData {

	public static final OHLCVData from(OHLCVDataRaw rawData) {
		return builder()
				.key(SymbolTimestampKey.from(rawData))
				.open(rawData.getOpen())
				.high(rawData.getHigh())
				.low(rawData.getLow())
				.close(rawData.getClose())
				.volume(rawData.getVolume())
				.build();
	}
	
	private SymbolTimestampKey key;
	
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
    public OHLCVData aggregateWith(OHLCVData other) {
    	
    	boolean isOtherOlder = this.key.getTimestamp() < other.key.getTimestamp();
    	
    	return OHLCVData.builder()
    			.key(key)
    			.open(isOtherOlder? this.open : other.open)
    			.high(this.high.doubleValue() > other.high.doubleValue()? this.high : other.high)
    			.low(this.low.doubleValue() < other.low.doubleValue()? this.low : other.low)
    			.close(isOtherOlder? other.close : this.close)
    			.volume(this.volume.longValue() + other.volume.longValue())
    			.build();
    }
}
