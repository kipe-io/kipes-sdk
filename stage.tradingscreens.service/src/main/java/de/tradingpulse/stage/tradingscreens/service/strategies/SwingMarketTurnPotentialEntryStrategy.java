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

public class SwingMarketTurnPotentialEntryStrategy
implements BiFunction<SymbolTimestampKey, ImpulseTradingScreenRecord, Iterable<SignalRecord>> {

	@Override
	public Iterable<SignalRecord> apply(SymbolTimestampKey key, ImpulseTradingScreenRecord value) {
		// Swing Potential Entry rules:
		// 
		// - longRange.current and longRange.change dictate trading direction
		// - long and short range directions/changes need to point in the same direction
		// - signals are generated on loss or change of direction
		//
		// longRange doesn't change -------------------------------------------
		// ENTRY(longRange.current) when
		//          longRange.change == NEUTRAL
		//      AND longRange.current != NEUTRAL
		//      AND shortRange.change == longRange.current
		//
		// ENTRY(shortRange.change) when
		//          longRange.change == NEUTRAL
		//      AND longRange.current == NEUTRAL
		//      AND shortRange.change != NEUTRAL
		//
		// longRange changes --------------------------------------------------
		// ENTRY(longRange.change) when
		//          longRange.change != NEUTRAL
		//      AND (
		//				(	shortRange.change == NEUTRAL
		//				AND (shortRange.direction == NEUTRAL OR shortRange.direction == longRange.change ))
		//			OR
		//				(	shortRange.change != NEUTRAL
		//				AND shortRange.change == longRange.change )
		
		List<SignalRecord> signals = new ArrayList<>(2);
		signals.add(evalEntrySignal(value));
		signals.removeIf(Objects::isNull);
		return signals;
	}
	

	private SignalRecord evalEntrySignal(ImpulseTradingScreenRecord value) {
		ImpulseRecord longRange = value.getLongRangeImpulseRecord();
		ImpulseRecord shortRange = value.getShortRangeImpulseRecord();
		
		if(	longRange.getChangeTradingDirection() == NEUTRAL
			&& longRange.getTradingDirection() != NEUTRAL
			&& shortRange.getChangeTradingDirection() == longRange.getTradingDirection())
		{
			return createEntrySignal(value, longRange.getTradingDirection());			
		}
		
		if(	longRange.getChangeTradingDirection() == NEUTRAL
			&& longRange.getTradingDirection() == NEUTRAL
			&& shortRange.getChangeTradingDirection() != NEUTRAL)
		{
			return createEntrySignal(value, shortRange.getChangeTradingDirection());			
		}
		
		if(	longRange.getChangeTradingDirection() != NEUTRAL
			&& (
					(shortRange.getChangeTradingDirection() == NEUTRAL
					&& (shortRange.getTradingDirection() == NEUTRAL || shortRange.getTradingDirection() == longRange.getChangeTradingDirection()))
				||	(shortRange.getChangeTradingDirection() != NEUTRAL
					&& shortRange.getChangeTradingDirection() == longRange.getChangeTradingDirection())
			)
		) {
			return createEntrySignal(value, longRange.getChangeTradingDirection());			
		}
		
		return null;
	}
	
	private SignalRecord createEntrySignal(ImpulseTradingScreenRecord value, TradingDirection direction) {
		return SignalRecord.builder()
				.key(value.getKey())
				.timeRange(value.getTimeRange())
				.strategyKey(SwingSignalType.SWING_MARKET_TURN_POTENTIAL.name())
				.signalType(SignalType.from(Type.ENTRY, direction))
				.build();			
		
	}
}
