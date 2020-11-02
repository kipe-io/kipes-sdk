package de.tradingpulse.stage.systems.recordtypes;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class ImpulseRecord extends AbstractIncrementalAggregateRecord {

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
	
	public boolean isSameImpulse(ImpulseRecord other) {
		return this.tradingDirection == other.tradingDirection
				&& this.lastTradingDirection == other.lastTradingDirection;
	}
}
