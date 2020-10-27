package de.tradingpulse.common.stream.aggregates;

import de.tradingpulse.common.stream.data.ImpulseData;
import de.tradingpulse.common.stream.data.TradingDirection;
import de.tradingpulse.common.stream.data.TradingScreenData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TradingScreenAggregate {

	private TradingScreenData tradingScreen;
	
	public TradingScreenData aggregate(ImpulseData weeklyImpulse, ImpulseData dailyImpulse) {
		if(weeklyImpulse == null || dailyImpulse == null) {
			this.tradingScreen = null;
			return null;
		}
	
		TradingDirection weekly = weeklyImpulse.getDirection();
		TradingDirection daily = dailyImpulse.getDirection();
		
		// if both are pointing in the same direction, we are the same
		TradingDirection direction = weekly == daily? daily : TradingDirection.NEUTRAL;

		this.tradingScreen = new TradingScreenData(
				dailyImpulse.getKey(), 
				tradingScreen == null? null : tradingScreen.getDirection(),
				direction);
		
		return this.tradingScreen;
	}
}
