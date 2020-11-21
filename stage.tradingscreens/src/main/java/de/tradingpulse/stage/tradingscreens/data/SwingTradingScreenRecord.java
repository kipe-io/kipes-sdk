package de.tradingpulse.stage.tradingscreens.data;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static de.tradingpulse.stage.tradingscreens.data.EntrySignal.ENTRY_LONG_MOMENTUM;
import static de.tradingpulse.stage.tradingscreens.data.EntrySignal.ENTRY_LONG_POTENTIAL;
import static de.tradingpulse.stage.tradingscreens.data.EntrySignal.ENTRY_SHORT_MOMENTUM;
import static de.tradingpulse.stage.tradingscreens.data.EntrySignal.ENTRY_SHORT_POTENTIAL;
import static de.tradingpulse.stage.tradingscreens.data.ExitSignal.EXIT_LONG;
import static de.tradingpulse.stage.tradingscreens.data.ExitSignal.EXIT_LONG_MOMENTUM;
import static de.tradingpulse.stage.tradingscreens.data.ExitSignal.EXIT_SHORT;
import static de.tradingpulse.stage.tradingscreens.data.ExitSignal.EXIT_SHORT_MOMENTUM;

import java.util.Optional;

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
		TradingDirection shortRangeChange = this.shortRangeImpulseRecord.getChangeTradingDirection();
		
		switch(longRangeChange) {
		case SHORT:		return getLongChangeShortEntrySignal();
		case NEUTRAL:	return getLongChangeNeutralEntrySignal();
		case LONG:		return getLongChangeLongEntrySignal();
		}
		
		throw new UnsupportedOperationException(
				String.format(
						"computation for long change '%s' and short change '%s' is not implemented", 
						longRangeChange,
						shortRangeChange));
	}
	
	private Optional<EntrySignal> getLongChangeShortEntrySignal() {
		
		TradingDirection longRangeChange = this.longRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection longRangeDirection = this.longRangeImpulseRecord.getTradingDirection();
		
		TradingDirection shortRangeChange = this.shortRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection shortRangeDirection = this.shortRangeImpulseRecord.getTradingDirection();

		switch(shortRangeChange) {
		case SHORT:
			switch(longRangeDirection) {
			case SHORT:		return shortRangeDirection == SHORT? Optional.of(ENTRY_SHORT_MOMENTUM) : Optional.of(ENTRY_SHORT_POTENTIAL);
			case NEUTRAL: 	return Optional.of(ENTRY_SHORT_POTENTIAL);
			case LONG:		break; // can't happen as long change is short
			}
			break;
			
		case NEUTRAL:
			switch(shortRangeDirection) {
			case SHORT:		return longRangeDirection == SHORT? Optional.of(ENTRY_SHORT_MOMENTUM) : Optional.of(ENTRY_SHORT_POTENTIAL);
			case NEUTRAL: 	return Optional.of(ENTRY_SHORT_POTENTIAL);
			case LONG:		return Optional.empty();
			}
			break;
			
		case LONG:
			return Optional.empty();
		}
		
		throw new UnsupportedOperationException(
				String.format(
						"computation for long change '%s' and short change '%s' is not implemented", 
						longRangeChange,
						shortRangeChange));
	}
	
	private Optional<EntrySignal> getLongChangeNeutralEntrySignal() {
		
		TradingDirection longRangeChange = this.longRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection longRangeDirection = this.longRangeImpulseRecord.getTradingDirection();
		
		TradingDirection shortRangeChange = this.shortRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection shortRangeDirection = this.shortRangeImpulseRecord.getTradingDirection();

		
		switch(shortRangeChange) {
		case SHORT:
			switch(longRangeDirection) {
			case SHORT: 	return shortRangeDirection == SHORT? Optional.of(ENTRY_SHORT_MOMENTUM) : Optional.of(ENTRY_SHORT_POTENTIAL);
			case NEUTRAL: 	return Optional.of(ENTRY_SHORT_POTENTIAL);
			case LONG:		return Optional.empty();
			}
			break;
			
		case NEUTRAL:	
			return Optional.empty();
		
		case LONG:		
			switch(longRangeDirection) {
			case SHORT: 	return Optional.empty();
			case NEUTRAL: 	return Optional.of(ENTRY_LONG_POTENTIAL);
			case LONG:		return shortRangeDirection == LONG? Optional.of(ENTRY_LONG_MOMENTUM) : Optional.of(ENTRY_LONG_POTENTIAL);
			}
			break;
		}
		
		throw new UnsupportedOperationException(
				String.format(
						"computation for long change '%s' and short change '%s' is not implemented", 
						longRangeChange,
						shortRangeChange));
	}
	
	private Optional<EntrySignal> getLongChangeLongEntrySignal() {
		
		TradingDirection longRangeChange = this.longRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection longRangeDirection = this.longRangeImpulseRecord.getTradingDirection();
		
		TradingDirection shortRangeChange = this.shortRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection shortRangeDirection = this.shortRangeImpulseRecord.getTradingDirection();

		switch(shortRangeChange) {
		case SHORT:
			return Optional.empty();
			
		case NEUTRAL:
			switch(shortRangeDirection) {
			case SHORT:		return Optional.empty();
			case NEUTRAL: 	return Optional.of(ENTRY_LONG_POTENTIAL);
			case LONG:		return longRangeDirection == LONG? Optional.of(ENTRY_LONG_MOMENTUM) : Optional.of(ENTRY_LONG_POTENTIAL);
			}
			break;
			
		case LONG:
			switch(longRangeDirection) {
			case SHORT:		break; // can't happen as long change is long
			case NEUTRAL: 	return Optional.of(ENTRY_LONG_POTENTIAL);
			case LONG:		return shortRangeDirection == LONG? Optional.of(ENTRY_LONG_MOMENTUM) : Optional.of(ENTRY_LONG_POTENTIAL);
			}
			break;
		}
		
		throw new UnsupportedOperationException(
				String.format(
						"computation for long change '%s' and short change '%s' is not implemented", 
						longRangeChange,
						shortRangeChange));
	}
	

	/**
	 * Returns an optional ExitSignal based on the configured long and short
	 * range ImpulseData.<br />
	 * <br />
	 * An exit signal will be generated, if 
	 * <ul>
	 *  <li>either long or short impulse data change their direction,</li>
	 *  <li>the both point in the different direction</li>
	 *  <li>long range dictates the direction</li>
	 * </ul>  
	 * 
	 * @return
	 */
	@JsonProperty(access = Access.READ_ONLY)
	public Optional<ExitSignal> getExitSignal() {
		// see test class for complete list of signals.
		// 
		// The 
		if(longRangeImpulseRecord == null || shortRangeImpulseRecord == null) {
			return Optional.empty();
		}
		
		TradingDirection longRangeChange = this.longRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection shortRangeChange = this.shortRangeImpulseRecord.getChangeTradingDirection();
		
		switch(longRangeChange) {
		case SHORT:		return getLongChangeShortExitSignal();
		case NEUTRAL:	return getLongChangeNeutralExitSignal();
		case LONG:		return getLongChangeLongExitSignal();
		}
		
		throw new UnsupportedOperationException(
				String.format(
						"computation for long change '%s' and short change '%s' is not implemented", 
						longRangeChange,
						shortRangeChange));
	}
	
	private Optional<ExitSignal> getLongChangeShortExitSignal() {
		
		TradingDirection longRangeChange = this.longRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection longRangeDirection = this.longRangeImpulseRecord.getTradingDirection();
		
		TradingDirection shortRangeChange = this.shortRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection shortRangeDirection = this.shortRangeImpulseRecord.getTradingDirection();

		switch(shortRangeChange) {
		case SHORT:
			switch(shortRangeDirection) {
			case SHORT:		return Optional.of(EXIT_LONG);
			case NEUTRAL: 	return longRangeDirection == SHORT? Optional.of(EXIT_LONG) : Optional.of(EXIT_LONG_MOMENTUM);
			case LONG:		return Optional.empty();
			}
			break;
			
		case NEUTRAL:
			switch(shortRangeDirection) {
			case SHORT:		return Optional.empty();
			case NEUTRAL: 	return longRangeDirection == NEUTRAL? Optional.empty() : Optional.of(EXIT_LONG);
			case LONG:		return longRangeDirection == SHORT? Optional.of(EXIT_LONG) : Optional.of(EXIT_LONG_MOMENTUM);
			}
			break;
			
		case LONG:
			return Optional.empty();
		}
		
		throw new UnsupportedOperationException(
				String.format(
						"computation for long change '%s' and short change '%s' is not implemented", 
						longRangeChange,
						shortRangeChange));
	}
	
	private Optional<ExitSignal> getLongChangeNeutralExitSignal() {
		
		TradingDirection longRangeChange = this.longRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection longRangeDirection = this.longRangeImpulseRecord.getTradingDirection();
		
		TradingDirection shortRangeChange = this.shortRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection shortRangeDirection = this.shortRangeImpulseRecord.getTradingDirection();

		switch(shortRangeChange) {
		case SHORT:
			switch(shortRangeDirection) {
			case SHORT:		return longRangeDirection == SHORT? Optional.empty() : Optional.of(EXIT_LONG);
			case NEUTRAL: 	return longRangeDirection == LONG? Optional.of(EXIT_LONG_MOMENTUM) : Optional.empty();
			case LONG:		break; // can't happen as short change is long
			}
			break;
			
		case NEUTRAL:
			return Optional.empty();
			
		case LONG:
			switch(shortRangeDirection) {
			case SHORT:		break; // can't happen as short change is long
			case NEUTRAL: 	return longRangeDirection == SHORT? Optional.of(EXIT_SHORT_MOMENTUM) : Optional.empty();
			case LONG:		return longRangeDirection == LONG? Optional.empty() : Optional.of(EXIT_SHORT);
			}
			break;
		}
		
		throw new UnsupportedOperationException(
				String.format(
						"computation for long change '%s' and short change '%s' is not implemented", 
						longRangeChange,
						shortRangeChange));
	}
	
	private Optional<ExitSignal> getLongChangeLongExitSignal() {
		
		TradingDirection longRangeChange = this.longRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection longRangeDirection = this.longRangeImpulseRecord.getTradingDirection();
		
		TradingDirection shortRangeChange = this.shortRangeImpulseRecord.getChangeTradingDirection();
		TradingDirection shortRangeDirection = this.shortRangeImpulseRecord.getTradingDirection();

		switch(shortRangeChange) {
		case SHORT:
			return Optional.empty();
			
		case NEUTRAL:
			switch(shortRangeDirection) {
			case SHORT:		return longRangeDirection == LONG? Optional.of(EXIT_SHORT) : Optional.of(EXIT_SHORT_MOMENTUM);
			case NEUTRAL: 	return longRangeDirection == NEUTRAL? Optional.empty(): Optional.of(EXIT_SHORT);
			case LONG:		return Optional.empty();
			}
			break;
			
		case LONG:
			switch(shortRangeDirection) {
			case SHORT:		return Optional.empty();
			case NEUTRAL: 	return longRangeDirection == LONG? Optional.of(EXIT_SHORT) : Optional.of(EXIT_SHORT_MOMENTUM);
			case LONG:		return Optional.of(EXIT_SHORT);
			}
		}
		
		throw new UnsupportedOperationException(
				String.format(
						"computation for long change '%s' and short change '%s' is not implemented", 
						longRangeChange,
						shortRangeChange));
	}
}
