package de.tradingpulse.stage.tradingscreens.service.strategies;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.EXIT_LONG;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.EXIT_SHORT;
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

class SwingMarketTurnPotentialExitStrategyTest {

	private SwingMarketTurnPotentialExitStrategy strategy = new SwingMarketTurnPotentialExitStrategy();
	
	@ParameterizedTest
	@MethodSource("applyTestData")
	void test(
			TradingDirection longLast,
			TradingDirection longCurrent,
			
			TradingDirection shortLast,
			TradingDirection shortCurrent,
			
			SignalType exit)
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
		
		if(exit != null) {
			assertTrue(signals.hasNext());
			SignalRecord signal = signals.next();
			
			assertEquals(exit, signal.getSignalType());
		}
		
		assertFalse(signals.hasNext());
	}
	
	static Stream<Arguments> applyTestData() {
		List<Arguments> args = new LinkedList<>();
		
		// EXIT longChange == NEUTRAL
		args.addAll(generateArguments(NEUTRAL, SHORT, 	EXIT_LONG));
		args.addAll(generateArguments(NEUTRAL, NEUTRAL, null));
		args.addAll(generateArguments(NEUTRAL, LONG, 	EXIT_SHORT));
		// EXIT longChange != NEUTRAL
		args.addAll(generateArguments(SHORT, 	SHORT, 	EXIT_LONG));
		args.addAll(generateArguments(SHORT, 	NEUTRAL,EXIT_LONG));
		args.addAll(generateArguments(SHORT, 	LONG, 	EXIT_LONG));
		
		args.addAll(generateArguments(LONG, 	SHORT, 	EXIT_SHORT));
		args.addAll(generateArguments(LONG, 	NEUTRAL,EXIT_SHORT));
		args.addAll(generateArguments(LONG, 	LONG, 	EXIT_SHORT));
		
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
								SignalType adjustedSignal = signal;
								if( shortCurrent == longCurrent
									|| (longChange == NEUTRAL && shortCurrent == NEUTRAL)
									|| (longChange != NEUTRAL && longCurrent == NEUTRAL && shortChange == NEUTRAL)
									|| (longLast != NEUTRAL && shortLast != NEUTRAL && longLast.opposite() == shortLast)
									|| (longChange != NEUTRAL && longCurrent == NEUTRAL && shortChange != NEUTRAL && shortLast == NEUTRAL && longLast == shortCurrent))
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
