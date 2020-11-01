package de.tradingpulse.stage.tradingscreens.data;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.*;
import static de.tradingpulse.stage.tradingscreens.data.EntrySignal.LONG_ENTRY;
import static de.tradingpulse.stage.tradingscreens.data.EntrySignal.SHORT_ENTRY;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

import de.tradingpulse.common.stream.recordtypes.ImpulseData;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SwingTradingScreenData {

	private SymbolTimestampKey key;
	private ImpulseData longRangeImpulseData;
	private	ImpulseData	shortRangeImpulseData;
	
	@JsonProperty(access = Access.READ_ONLY)
	public TradingDirection getLastTradingDirection() {
		return this.longRangeImpulseData.getLastTradingDirection();
	}
	
	@JsonProperty(access = Access.READ_ONLY)
	public TradingDirection getTradingDirection() {
		return this.longRangeImpulseData.getTradingDirection();
	}

	/**
	 * Returns an optional EntrySignal based on the configured long and short
	 * range ImpulseData.<br />
	 * <br />
	 * An entry signal will be generated, if 
	 * <ul>
	 *  <li>either long or short impulse data change their direction,</li>
	 *  <li>the both point in the same direction</li>
	 *  <li>long range dictates the direction</li>
	 * </ul>  
	 * 
	 * @return
	 */
	@JsonProperty(access = Access.READ_ONLY)
	public Optional<EntrySignal> getEntrySignal() {
		// see test class for complete list of signals.
		// 
		// The 
		if(longRangeImpulseData == null || shortRangeImpulseData == null) {
			return Optional.empty();
		}
		
		TradingDirection longRangeChange = this.longRangeImpulseData.getChangeTradingDirection();
		TradingDirection longRangeDirection = this.longRangeImpulseData.getTradingDirection();
		
		TradingDirection shortRangeChange = this.shortRangeImpulseData.getChangeTradingDirection();
		TradingDirection shortRangeDirection = this.shortRangeImpulseData.getTradingDirection();
		
		if(longRangeChange == SHORT) {
			switch(shortRangeChange) {
			case SHORT:		return Optional.of(SHORT_ENTRY);
			case NEUTRAL:	return shortRangeDirection == LONG? Optional.empty() : Optional.of(SHORT_ENTRY);
			case LONG:		return Optional.empty();
			}
		}
		
		if(longRangeChange == NEUTRAL) {
			switch(shortRangeChange) {
			case SHORT:		return longRangeDirection == LONG? Optional.empty() : Optional.of(SHORT_ENTRY);
			case NEUTRAL:	return Optional.empty();
			case LONG:		return longRangeDirection == SHORT? Optional.empty() : Optional.of(LONG_ENTRY);
			}
		}
		
		if(longRangeChange == LONG) {
			switch(shortRangeChange) {
			case SHORT:		return Optional.empty();
			case NEUTRAL:	return shortRangeDirection == SHORT? Optional.empty() : Optional.of(LONG_ENTRY);
			case LONG:		return Optional.of(LONG_ENTRY);
			}
		}
		
		throw new UnsupportedOperationException(
				String.format(
						"computation for long change '%s' and short change '%s' is not implemented", 
						longRangeChange,
						shortRangeChange));
	}
	
	@JsonIgnore
	public Optional<ExitSignal> getExitSignal() {
		throw new UnsupportedOperationException("not yet implemented");
	}
}
