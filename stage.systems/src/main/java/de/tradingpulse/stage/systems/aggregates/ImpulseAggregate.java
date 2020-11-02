package de.tradingpulse.stage.systems.aggregates;

import de.tradingpulse.common.stream.aggregates.DeepCloneable;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.recordtypes.ImpulseData;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ImpulseAggregate implements DeepCloneable<ImpulseAggregate>{

	private ImpulseData impulseData;
	
	public ImpulseAggregate deepClone() {
		ImpulseAggregate clone = new ImpulseAggregate();
		
		if(this.impulseData == null) {
			return clone;
		}
		
		SymbolTimestampKey keyClone = impulseData.getKey() == null? null : SymbolTimestampKey.builder()
				.symbol(impulseData.getKey().getSymbol())
				.timestamp(impulseData.getKey().getTimestamp())
				.build();
		
		clone.impulseData = ImpulseData.builder()
				.key(keyClone)
				.lastTradingDirection(impulseData.getLastTradingDirection())
				.tradingDirection(impulseData.getTradingDirection())
				.build();
		
		return clone;
	}
	
	public ImpulseData aggregate(DoubleRecord emaData, MACDHistogramRecord macdHistogramData) {
		
		TradingDirection tradingDirection = null;
		
		if(emaData == null || macdHistogramData == null || emaData.getVChange() == null || macdHistogramData.getHChange() == null) {
			this.impulseData = null;
			return null;
		}
		
		if(emaData.getVChange() > 0 && macdHistogramData.getHChange() > 0) {
			// if both indicators raise then it's a long
			tradingDirection = TradingDirection.LONG;
			
		} else if(emaData.getVChange() < 0 && macdHistogramData.getHChange() < 0) {
			// if both indicators fall then it's a short
			tradingDirection = TradingDirection.SHORT;
		} else {
			tradingDirection = TradingDirection.NEUTRAL;
		}
		
		this.impulseData = ImpulseData.builder()
				.tradingDirection(tradingDirection)
				.lastTradingDirection(this.impulseData == null? null : this.impulseData.getTradingDirection())
				.build();
		
		return this.impulseData;
	}
}
