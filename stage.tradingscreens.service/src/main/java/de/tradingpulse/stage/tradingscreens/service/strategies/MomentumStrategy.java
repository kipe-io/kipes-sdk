package de.tradingpulse.stage.tradingscreens.service.strategies;

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

public class MomentumStrategy
implements BiFunction<SymbolTimestampKey, ImpulseTradingScreenRecord, Iterable<SignalRecord>> {

	@Override
	public Iterable<SignalRecord> apply(SymbolTimestampKey key, ImpulseTradingScreenRecord value) {
		// Momentum rules:
		// 
		// - longRange.current dictates trading direction
		// - longRange.current == NEUTRAL means no ENTRY
		//
		// ENTRY(longRange.current) when
		//          longRange.current != NEUTRAL
		//      AND shortRange.change = longRange.current
		//      AND shortRange.current = longRange.current
		//
		// EXIT(longRange.current) when
		//          longRange.current != NEUTRAL
		//		AND longRange.change == NEUTRAL
		//      AND shortRange.change != NEUTRAL
		//      AND shortRange.last == longRange.last
		//		AND shortRange.change != longRange.current
		//
		// EXIT(longRange.last) when
		//          longRange.last != NEUTRAL
		//      AND longRange.change != NEUTRAL
		//      AND shortRange.last == longRange.last
		
		List<SignalRecord> signals = new ArrayList<>(2);
		signals.add(evalExitSignal(value));
		signals.add(evalEntrySignal(value));
		signals.removeIf(Objects::isNull);
		return signals;
	}

	private SignalRecord evalExitSignal(ImpulseTradingScreenRecord value) {
		ImpulseRecord longRange = value.getLongRangeImpulseRecord();
		ImpulseRecord shortRange = value.getShortRangeImpulseRecord();
		
		if(	longRange.getTradingDirection() != TradingDirection.NEUTRAL
			&& longRange.getChangeTradingDirection() == TradingDirection.NEUTRAL
			&& shortRange.getChangeTradingDirection() != TradingDirection.NEUTRAL
			&& shortRange.getLastTradingDirection() == longRange.getLastTradingDirection()
			&& shortRange.getChangeTradingDirection() != longRange.getTradingDirection())
		{
			return SignalRecord.builder()
					.key(value.getKey())
					.timeRange(value.getTimeRange())
					.strategyKey(SwingSignalType.SWING_MOMENTUM.name())
					.signalType(SignalType.from(Type.EXIT, longRange.getTradingDirection()))
					.build();
		}
		
		if(	longRange.getLastTradingDirection() != TradingDirection.NEUTRAL
			&& longRange.getChangeTradingDirection() != TradingDirection.NEUTRAL
			&& shortRange.getLastTradingDirection() == longRange.getLastTradingDirection())
		{
			return SignalRecord.builder()
					.key(value.getKey())
					.timeRange(value.getTimeRange())
					.strategyKey(SwingSignalType.SWING_MOMENTUM.name())
					.signalType(SignalType.from(Type.EXIT, longRange.getLastTradingDirection()))
					.build();
		}
		
		return null;
	}

	private SignalRecord evalEntrySignal(ImpulseTradingScreenRecord value) {
		ImpulseRecord longRange = value.getLongRangeImpulseRecord();
		ImpulseRecord shortRange = value.getShortRangeImpulseRecord();
		
		if(	longRange.getTradingDirection() != TradingDirection.NEUTRAL
			&& shortRange.getChangeTradingDirection() == longRange.getTradingDirection()
			&& shortRange.getTradingDirection() == longRange.getTradingDirection())
		{
			return SignalRecord.builder()
					.key(value.getKey())
					.timeRange(value.getTimeRange())
					.strategyKey(SwingSignalType.SWING_MOMENTUM.name())
					.signalType(SignalType.from(Type.ENTRY, longRange.getTradingDirection()))
					.build();
		}
		
		return null;
	}
	
}