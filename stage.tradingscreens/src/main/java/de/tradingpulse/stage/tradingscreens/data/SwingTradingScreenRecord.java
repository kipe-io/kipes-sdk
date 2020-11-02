package de.tradingpulse.stage.tradingscreens.data;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static de.tradingpulse.stage.tradingscreens.data.EntrySignal.LONG_ENTRY;
import static de.tradingpulse.stage.tradingscreens.data.EntrySignal.SHORT_ENTRY;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
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
public class SwingTradingScreenRecord extends AbstractIncrementalAggregateRecord {

	private ImpulseRecord longRangeImpulseRecord;
	private	ImpulseRecord shortRangeImpulseRecord;
	
	@JsonProperty(access = Access.READ_ONLY)
	public TradingDirection getLastTradingDirection() {
		return this.longRangeImpulseRecord.getLastTradingDirection();
	}
	
	@JsonProperty(access = Access.READ_ONLY)
	public TradingDirection getTradingDirection() {
		return this.longRangeImpulseRecord.getTradingDirection();
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
		if(longRangeImpulseRecord == null || shortRangeImpulseRecord == null) {
			return Optional.empty();
		}
		
		TradingDirection longRangeChange = this.longRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection longRangeDirection = this.longRangeImpulseRecord.getTradingDirection();
		
		TradingDirection shortRangeChange = this.shortRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection shortRangeDirection = this.shortRangeImpulseRecord.getTradingDirection();
		
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
