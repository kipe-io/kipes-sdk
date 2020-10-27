package de.tradingpulse.common.stream.aggregates;

import de.tradingpulse.common.stream.data.DoubleData;
import de.tradingpulse.common.stream.data.ImpulseData;
import de.tradingpulse.common.stream.data.MACDHistogramData;
import de.tradingpulse.common.stream.data.SymbolTimestampKey;
import de.tradingpulse.common.stream.data.TradingDirection;
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
				.lastDirection(impulseData.getLastDirection())
				.direction(impulseData.getDirection())
				.build();
		
		return clone;
	}
	
	public ImpulseData aggregate(DoubleData emaData, MACDHistogramData macdHistogramData) {
		
		TradingDirection direction = null;
		
		if(emaData == null || macdHistogramData == null || emaData.getVChange() == null || macdHistogramData.getHChange() == null) {
			this.impulseData = null;
			return null;
		}
		
		if(emaData.getVChange() > 0 && macdHistogramData.getHChange() > 0) {
			// if both indicators raise then it's a long
			direction = TradingDirection.LONG;
			
		} else if(emaData.getVChange() < 0 && macdHistogramData.getHChange() < 0) {
			// if both indicators fall then it's a short
			direction = TradingDirection.SHORT;
		} else {
			direction = TradingDirection.NEUTRAL;
		}
		
		this.impulseData = ImpulseData.builder()
				.direction(direction)
				.lastDirection(this.impulseData == null? null : this.impulseData.getDirection())
				.build();
		
		return this.impulseData;
	}
}
