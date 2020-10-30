package de.tradingpulse.common.stream.data;

import static de.tradingpulse.common.stream.data.TradingDirection.LONG;
import static de.tradingpulse.common.stream.data.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.data.TradingDirection.SHORT;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ImpulseData {

	private SymbolTimestampKey key;
	private TradingDirection tradingDirection;
	private TradingDirection lastTradingDirection;
	
	@JsonProperty(access = Access.READ_ONLY)
	public TradingDirection getChangeTradingDirection() {
		if(lastTradingDirection == null || tradingDirection == null) {
			return NEUTRAL;
		}
		
		if(lastTradingDirection == SHORT) {
			switch(tradingDirection) {
			case SHORT:		return NEUTRAL;
			case NEUTRAL:	return LONG;
			case LONG:		return LONG;
			}
		}
		
		if(lastTradingDirection == NEUTRAL) {
			switch(tradingDirection) {
			case SHORT:		return SHORT;
			case NEUTRAL:	return NEUTRAL;
			case LONG:		return LONG;
			}
		}
		
		if(lastTradingDirection == LONG) {
			switch(tradingDirection) {
			case SHORT:		return SHORT;
			case NEUTRAL:	return SHORT;
			case LONG:		return NEUTRAL;
			}
		}
		
		throw new UnsupportedOperationException(
				String.format(
						"computation for lastTradingDirection '%s' and tradingDirection '%s' is not implemented", 
						lastTradingDirection,
						tradingDirection));
	}
	
	public boolean isSameImpulse(ImpulseData other) {
		return this.tradingDirection == other.tradingDirection
				&& this.lastTradingDirection == other.lastTradingDirection;
	}
}
