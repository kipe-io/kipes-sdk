package de.tradingpulse.stage.backtest.recordtypes;

import static de.tradingpulse.common.utils.MathUtils.round;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
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
	private Double entryValue;
	private Double exitValue;
	
	@JsonProperty(access = Access.READ_ONLY)
	public Double getRevenue() {
		if(this.entryValue == null || this.exitValue == null) {
			return null;
		}
		
		int sign = this.tradingDirection == TradingDirection.SHORT? -1 : 1;
		
		return round(sign * (this.exitValue - this.entryValue), 2);
	}
	
	@JsonProperty(access = Access.READ_ONLY)
	public Double getRevenueRatio() {
		Double revenue = getRevenue();
		if(revenue == null) {
			return null;
		}
		return round(revenue / this.entryValue, 4); 
	}
	
	@JsonProperty(access = Access.READ_ONLY)
	public long getDurationMS() {
		return getKey().getTimestamp() - entryTimestamp;
	}
}
