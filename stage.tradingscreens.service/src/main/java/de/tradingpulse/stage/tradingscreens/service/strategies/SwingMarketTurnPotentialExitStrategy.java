package de.tradingpulse.stage.tradingscreens.service.strategies;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType;
import de.tradingpulse.stage.tradingscreens.recordtypes.SwingSignalType;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.Type;

public class SwingMarketTurnPotentialExitStrategy
implements BiFunction<SymbolTimestampKey, ImpulseTradingScreenRecord, Iterable<SignalRecord>> {

	@Override
	public Iterable<SignalRecord> apply(SymbolTimestampKey key, ImpulseTradingScreenRecord value) {
		// Swing Potential Exit rules:
		// 
		// - longRange.current and longRange.change dictate trading direction
		// - long and short range directions/changes need to point in the same direction
		// - signals are generated when one direction points against the trade 
		// - no signals when last directions are opposites
		//
		// longRange doesn't change -------------------------------------------
		// EXIT(shortRange.current.opposite) when
		//          longRange.change == NEUTRAL
		//      AND longRange.current != NEUTRAL
		//      AND shortRange.change != NEUTRAL
		//      AND shortRange.current != NEUTRAL
		//      AND shortRange.current != longRange.current
		//
		// EXIT(shortRange.current.opposite) when
		//          longRange.change == NEUTRAL
		//      AND longRange.current == NEUTRAL
		//      AND shortRange.change != NEUTRAL
		//      AND shortRange.current != NEUTRAL
		//
		// longRange changes --------------------------------------------------
		// EXIT(longRange.current.opposite) when
		//          longRange.change != NEUTRAL
		//      AND longRange.current != NEUTRAL
		//      AND (shortRange.current == NEUTRAL OR shortRange.current != longRange.current)
		//
		// EXIT(longRange.change.opposite) when
		//          longRange.change != NEUTRAL
		//      AND longRange.current == NEUTRAL
		//      AND shortRange.current == longRange.change
		//
		
		List<SignalRecord> signals = new ArrayList<>(2);
		signals.add(evalExitSignal(value));
		signals.removeIf(Objects::isNull);
		return signals;
	}
	
	private SignalRecord evalExitSignal(ImpulseTradingScreenRecord value) {
		ImpulseRecord longRange = value.getLongRangeImpulseRecord();
		ImpulseRecord shortRange = value.getShortRangeImpulseRecord();
		
		if( longRange.getLastTradingDirection() != NEUTRAL
			&& shortRange.getLastTradingDirection() != NEUTRAL
			&& longRange.getLastTradingDirection().opposite() == shortRange.getLastTradingDirection())
		{
			return null;
		}
		
		if(	longRange.getChangeTradingDirection() == NEUTRAL
			&& longRange.getTradingDirection() != NEUTRAL
			&& shortRange.getChangeTradingDirection() != NEUTRAL
			&& shortRange.getTradingDirection() != NEUTRAL
			&& shortRange.getTradingDirection() != longRange.getTradingDirection())
		{
			return createExitSignal(value, shortRange.getTradingDirection().opposite());			
		}
		
		if( longRange.getChangeTradingDirection() == NEUTRAL
			&& longRange.getTradingDirection() == NEUTRAL
			&& shortRange.getChangeTradingDirection() != NEUTRAL
			&& shortRange.getTradingDirection() != NEUTRAL )
		{
			return createExitSignal(value, shortRange.getTradingDirection().opposite());			
		}
		
		if( longRange.getChangeTradingDirection() != NEUTRAL
			&& longRange.getTradingDirection() != NEUTRAL
			&& (shortRange.getTradingDirection() == NEUTRAL || shortRange.getTradingDirection() != longRange.getTradingDirection()))
		{
			return createExitSignal(value, longRange.getTradingDirection().opposite());			
		}
		
		
		if( longRange.getChangeTradingDirection() != NEUTRAL
			&& longRange.getTradingDirection() == NEUTRAL
			&& shortRange.getTradingDirection() == longRange.getChangeTradingDirection())
		{
			return createExitSignal(value, longRange.getChangeTradingDirection().opposite());			
		}
		
		return null;
	}
	
	private SignalRecord createExitSignal(ImpulseTradingScreenRecord value, TradingDirection direction) {
		return SignalRecord.builder()
				.key(value.getKey())
				.timeRange(value.getTimeRange())
				.strategyKey(SwingSignalType.SWING_MARKET_TURN_POTENTIAL.name())
				.signalType(SignalType.from(Type.EXIT, direction))
				.build();			
		
	}
}
