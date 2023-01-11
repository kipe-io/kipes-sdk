package de.tradingpulse.stage.backtest.recordtypes;

import static io.kipe.common.utils.MathUtils.round;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.recordtypes.Trends;
import lombok.AllArgsConstructor;
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
public class BacktestResultRecord extends AbstractIncrementalAggregateRecord {

	private String strategyKey;
	private TradingDirection tradingDirection;
	private long entryTimestamp;
	private Double entry;
	private Double high;
	private Double low;
	private Double exit;
	private Trends shortRangeTrends;
	private Trends longRangeTrends;
	
	@JsonProperty(access = Access.READ_ONLY)
	public Double getRevenue() {
		if(this.entry == null || this.exit == null) {
			return null;
		}
		
		int sign = this.tradingDirection == TradingDirection.SHORT? -1 : 1;
		
		return round(sign * (this.exit - this.entry), 2);
	}
	
	@JsonProperty(access = Access.READ_ONLY)
	public Double getMaxPotentialRevenue() {
		if(this.entry == null || this.high == null || this.low == null) {
			return null;
		}
		
		if(this.tradingDirection == TradingDirection.LONG) {
			return round(this.high - this.entry, 2);
		} else {
			return round(this.entry - this.low, 2);
		}
	}
	
	@JsonProperty(access = Access.READ_ONLY)
	public long getDurationMS() {
		return getKey().getTimestamp() - entryTimestamp;
	}
}
