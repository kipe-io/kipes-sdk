package de.tradingpulse.optimus;

import de.tradingpulse.stage.backtest.recordtypes.BacktestResultRecord;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@NoArgsConstructor
@SuperBuilder
public class Trade extends BacktestResultRecord {

	public static Trade from(BacktestResultRecord record) {
		return builder()
				.key(record.getKey())
				.timeRange(record.getTimeRange())
				.strategyKey(record.getStrategyKey())
				.tradingDirection(record.getTradingDirection())
				.entryTimestamp(record.getEntryTimestamp())
				.entry(record.getEntry())
				.high(record.getHigh())
				.low(record.getLow())
				.exit(record.getExit())
				.shortRangeTrends(record.getShortRangeTrends())
				.longRangeTrends(record.getLongRangeTrends())
				
				.revRatio(record.getRevenue()/record.getEntry())
				.build();
	}
	
	private double revRatio;
}
