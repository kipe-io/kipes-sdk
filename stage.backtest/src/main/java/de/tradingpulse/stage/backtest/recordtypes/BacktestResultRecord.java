package de.tradingpulse.stage.backtest.recordtypes;

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
		int sign = tradingDirection == TradingDirection.SHORT? -1 : 1;
		return entryValue == null || exitValue == null ? null : 
			sign * (exitValue - entryValue);
	}
	
	@JsonProperty(access = Access.READ_ONLY)
	public Double getRevenueRatio() {
		Double revenue = getRevenue();
		return revenue == null? null : 
			revenue / entryValue;
	}
}
