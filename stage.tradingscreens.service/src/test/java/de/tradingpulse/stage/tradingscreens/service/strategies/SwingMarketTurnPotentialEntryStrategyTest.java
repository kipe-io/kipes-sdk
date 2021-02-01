package de.tradingpulse.stage.tradingscreens.service.strategies;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.ENTRY_LONG;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.ENTRY_SHORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType;

class SwingMarketTurnPotentialEntryStrategyTest {

	private SwingMarketTurnPotentialEntryStrategy strategy = new SwingMarketTurnPotentialEntryStrategy();
	
	@ParameterizedTest
	@MethodSource("applyTestData")
	void test(
			TradingDirection longLast,
			TradingDirection longCurrent,
			
			TradingDirection shortLast,
			TradingDirection shortCurrent,
			
			SignalType entry)
	{
		ImpulseTradingScreenRecord record = ImpulseTradingScreenRecord.builder()
				.key(new SymbolTimestampKey())
				.longRangeImpulseRecord(ImpulseRecord.builder()
						.lastTradingDirection(longLast)
						.tradingDirection(longCurrent)
						.build())
				.shortRangeImpulseRecord(ImpulseRecord.builder()
						.lastTradingDirection(shortLast)
						.tradingDirection(shortCurrent)
						.build())
				.build();
		
		Iterator<SignalRecord> signals = strategy.apply(null, record).iterator();
		
		if(entry != null) {
			assertTrue(signals.hasNext());
			SignalRecord signal = signals.next();
			
			assertEquals(entry, signal.getSignalType());
		}
		
		assertFalse(signals.hasNext());
	}

	
	static Stream<Arguments> applyTestData() {
		List<Arguments> args = new LinkedList<>();
		
		// ENTRIES longChange == NEUTRAL
		args.addAll(generateArguments(NEUTRAL, SHORT, 	ENTRY_SHORT));
		args.addAll(generateArguments(NEUTRAL, NEUTRAL, null));
		args.addAll(generateArguments(NEUTRAL, LONG, 	ENTRY_LONG));
		// ENTRIES longChange != NEUTRAL
		args.addAll(generateArguments(SHORT, 	SHORT, 	ENTRY_SHORT));
		args.addAll(generateArguments(SHORT, 	NEUTRAL,ENTRY_SHORT));
		args.addAll(generateArguments(SHORT, 	LONG, 	null));
		
		args.addAll(generateArguments(LONG, 	SHORT, 	null));
		args.addAll(generateArguments(LONG, 	NEUTRAL,ENTRY_LONG));
		args.addAll(generateArguments(LONG, 	LONG, 	ENTRY_LONG));
		
		return args.stream();
	}
	
	private static List<Arguments> generateArguments(TradingDirection longChange, TradingDirection shortChange, SignalType signal) {
		List<Arguments> args = new LinkedList<>();
		for(TradingDirection longLast: TradingDirection.values()) {
			for(TradingDirection longCurrent: TradingDirection.values()) {
				
				if(TradingDirection.getChangeDirection(longLast, longCurrent) == longChange) {
					
					for(TradingDirection shortLast: TradingDirection.values()) {
						for(TradingDirection shortCurrent: TradingDirection.values()) {
							
							if(	TradingDirection.getChangeDirection(shortLast, shortCurrent) == shortChange )
							{
								TradingDirection sd = signal == null? null : signal.getTradingDirection();
								SignalType adjustedSignal = signal;
								if(		(longChange == NEUTRAL && longCurrent != NEUTRAL && longCurrent != sd)
									||  (shortChange == NEUTRAL && shortCurrent != NEUTRAL && shortCurrent != sd))
								{
									adjustedSignal = null;
								}
								args.add(arguments(longLast,longCurrent, shortLast,shortCurrent, adjustedSignal));
							}
						}
					}
				}
			}
		}
		
		return args;
	}
}
