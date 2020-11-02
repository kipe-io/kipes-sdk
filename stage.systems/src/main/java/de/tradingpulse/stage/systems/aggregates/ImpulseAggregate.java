package de.tradingpulse.stage.systems.aggregates;

import de.tradingpulse.common.stream.aggregates.DeepCloneable;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ImpulseAggregate implements DeepCloneable<ImpulseAggregate>{

	private ImpulseRecord impulseRecord;
	
	public ImpulseAggregate deepClone() {
		ImpulseAggregate clone = new ImpulseAggregate();
		
		if(this.impulseRecord == null) {
			return clone;
		}
		
		SymbolTimestampKey keyClone = impulseRecord.getKey() == null? null : SymbolTimestampKey.builder()
				.symbol(impulseRecord.getKey().getSymbol())
				.timestamp(impulseRecord.getKey().getTimestamp())
				.build();
		
		clone.impulseRecord = ImpulseRecord.builder()
				.key(keyClone)
				.timeRange(impulseRecord.getTimeRange())
				.lastTradingDirection(impulseRecord.getLastTradingDirection())
				.tradingDirection(impulseRecord.getTradingDirection())
				.build();
		
		return clone;
	}
	
	public ImpulseRecord aggregate(DoubleRecord emaRecord, MACDHistogramRecord macdHistogramRecord) {
		
		TradingDirection tradingDirection = null;
		
		if(emaRecord == null || macdHistogramRecord == null || emaRecord.getVChange() == null || macdHistogramRecord.getHChange() == null) {
			this.impulseRecord = null;
			return null;
		}
		
		if(emaRecord.getVChange() > 0 && macdHistogramRecord.getHChange() > 0) {
			// if both indicators raise then it's a long
			tradingDirection = TradingDirection.LONG;
			
		} else if(emaRecord.getVChange() < 0 && macdHistogramRecord.getHChange() < 0) {
			// if both indicators fall then it's a short
			tradingDirection = TradingDirection.SHORT;
		} else {
			tradingDirection = TradingDirection.NEUTRAL;
		}
		
		this.impulseRecord = ImpulseRecord.builder()
				.key(emaRecord.getKey())
				.timeRange(emaRecord.getTimeRange())
				.tradingDirection(tradingDirection)
				.lastTradingDirection(this.impulseRecord == null? null : this.impulseRecord.getTradingDirection())
				.build();
		
		return this.impulseRecord;
	}
}
