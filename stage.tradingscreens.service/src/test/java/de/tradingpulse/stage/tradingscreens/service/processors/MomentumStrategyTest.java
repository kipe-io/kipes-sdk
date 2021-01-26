package de.tradingpulse.stage.tradingscreens.service.processors;

import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.*;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Iterator;
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
import de.tradingpulse.stage.tradingscreens.service.processors.SignalsProcessor.MomentumStrategy;


class MomentumStrategyTest {

	private MomentumStrategy strategy = new MomentumStrategy();
	
	@ParameterizedTest
	@MethodSource("applyTestData")
	void test_apply(
			TradingDirection longTrend,
			TradingDirection longLast,
			TradingDirection longCurrent,

			TradingDirection shortTrend,
			TradingDirection shortLast,
			TradingDirection shortCurrent,

			SignalType entry,
			SignalType exit)
	{
		ImpulseTradingScreenRecord record = ImpulseTradingScreenRecord.builder()
				.key(new SymbolTimestampKey())
				.longRangeImpulseRecord(ImpulseRecord.builder()
						.lastTrend(longTrend)
						.lastTradingDirection(longLast)
						.tradingDirection(longCurrent)
						.build())
				.shortRangeImpulseRecord(ImpulseRecord.builder()
						.lastTrend(shortTrend)
						.lastTradingDirection(shortLast)
						.tradingDirection(shortCurrent)
						.build())
				.build();
		
		Iterator<SignalRecord> signals = strategy.apply(null, record).iterator();
		
		// we implicitly assume order of exit, entry
		if(exit != null) {
			assertTrue(signals.hasNext());
			SignalRecord signal = signals.next();
			
			assertEquals(exit, signal.getSignalType());
		}
		
		if(entry != null) {
			assertTrue(signals.hasNext());
			SignalRecord signal = signals.next();
			
			assertEquals(entry, signal.getSignalType());
		}
		
		assertFalse(signals.hasNext());
	}
	
	static Stream<Arguments> applyTestData() {
		return Stream.of(
				
				//			long						short						signals
				//			trend	last	current		trend	last	current		entry					exit
				
				// ENTRIES --------------------------------------------------------------------------------------------------------------
				arguments(	null,	null,	SHORT,		null,	NEUTRAL,SHORT,		ENTRY_SHORT,			null),					// 1
				arguments(	null,	null,	SHORT,		null,	LONG,	SHORT,		ENTRY_SHORT,			null),
				
				arguments(	null,	null,	NEUTRAL,	null,	null,	null,		null,					null),					// 3
				
				arguments(	null,	null,	LONG,		null,	NEUTRAL,LONG,		ENTRY_LONG,				null),					// 4
				arguments(	null,	null,	LONG,		null,	SHORT,	LONG,		ENTRY_LONG,				null),
				
				// EXIT ON shortRange change --------------------------------------------------------------------------------------------
				arguments(	null,	SHORT,	SHORT,		null,	SHORT,	NEUTRAL,	null,					EXIT_SHORT),			// 6
				
				arguments(	null,	LONG,	LONG,		null,	LONG,	NEUTRAL,	null,					EXIT_LONG),				// 7
				
				// EXIT ON longRange change --------------------------------------------------------------------------------------------
				arguments(	null,	SHORT,	NEUTRAL,	null,	SHORT,	null,		null,					EXIT_SHORT),			// 8
				arguments(	null,	SHORT,	LONG,		null,	SHORT,	null,		null,					EXIT_SHORT),			
				
				arguments(	null,	LONG,	NEUTRAL,	null,	LONG,	null,		null,					EXIT_LONG),				// 10
				arguments(	null,	LONG,	SHORT,		null,	LONG,	null,		null,					EXIT_LONG),			
				
				
				// ------------------------ SHORT: all trades in that direction ----------------------------------------------------------
				
				arguments(	SHORT,	SHORT,	SHORT,		SHORT,	SHORT,	SHORT,		null,					null),					// 12
				arguments(	SHORT,	SHORT,	SHORT,		SHORT,	SHORT,	NEUTRAL,	null,					EXIT_SHORT),					
				arguments(	SHORT,	SHORT,	SHORT,		SHORT,	SHORT,	LONG,		null,					EXIT_SHORT), 					
				
				arguments(	SHORT,	SHORT,	SHORT,		SHORT,	NEUTRAL,SHORT,		ENTRY_SHORT,			null),					// 15
				arguments(	SHORT,	SHORT,	SHORT,		SHORT,	NEUTRAL,NEUTRAL,	null,					null),					
				arguments(	SHORT,	SHORT,	SHORT,		SHORT,	NEUTRAL,LONG,		null,					null), 					
				
				arguments(	SHORT,	SHORT,	SHORT,		SHORT,	LONG,	SHORT,		ENTRY_SHORT,			null), // invalid		// 18
				arguments(	SHORT,	SHORT,	SHORT,		SHORT,	LONG,	NEUTRAL,	null,					null), // invalid: short.trend != short.last 				
				arguments(	SHORT,	SHORT,	SHORT,		SHORT,	LONG,	LONG,		null,					null), // invalid: short.trend != short.last
				

				arguments(	SHORT,	SHORT,	SHORT,		null,	SHORT,	SHORT,		null,					null),					// 21
				arguments(	SHORT,	SHORT,	SHORT,		null,	SHORT,	NEUTRAL,	null,					EXIT_SHORT),					
				arguments(	SHORT,	SHORT,	SHORT,		null,	SHORT,	LONG,		null,					EXIT_SHORT), 					
				
				arguments(	SHORT,	SHORT,	SHORT,		null,	NEUTRAL,SHORT,		ENTRY_SHORT,			null),					// 24
				arguments(	SHORT,	SHORT,	SHORT,		null,	NEUTRAL,NEUTRAL,	null,					null),					
				arguments(	SHORT,	SHORT,	SHORT,		null,	NEUTRAL,LONG,		null,					null), 					
				
				arguments(	SHORT,	SHORT,	SHORT,		null,	LONG,	SHORT,		ENTRY_SHORT,			null),					// 27
				arguments(	SHORT,	SHORT,	SHORT,		null,	LONG,	NEUTRAL,	null,					null),					
				arguments(	SHORT,	SHORT,	SHORT,		null,	LONG,	LONG,		null,					null), 					

				
				arguments(	SHORT,	SHORT,	SHORT,		LONG,	SHORT,	SHORT,		null,					null), // invalid		// 30
				arguments(	SHORT,	SHORT,	SHORT,		LONG,	SHORT,	NEUTRAL,	null,					EXIT_SHORT), // invalid: short.trend != short.last
				arguments(	SHORT,	SHORT,	SHORT,		LONG,	SHORT,	LONG,		null,					EXIT_SHORT), // invalid: short.trend != short.last
				
				arguments(	SHORT,	SHORT,	SHORT,		LONG,	NEUTRAL,SHORT,		ENTRY_SHORT,			null),					// 33
				arguments(	SHORT,	SHORT,	SHORT,		LONG,	NEUTRAL,NEUTRAL,	null,					null),					
				arguments(	SHORT,	SHORT,	SHORT,		LONG,	NEUTRAL,LONG,		null,					null), 					
				
				arguments(	SHORT,	SHORT,	SHORT,		LONG,	LONG,	SHORT,		ENTRY_SHORT,			null),					// 36
				arguments(	SHORT,	SHORT,	SHORT,		LONG,	LONG,	NEUTRAL,	null,					null),					
				arguments(	SHORT,	SHORT,	SHORT,		LONG,	LONG,	LONG,		null,					null), 					

				
				arguments(	LONG,	LONG,	LONG,		LONG,	LONG,	LONG,		null,					null)					
			);
	}

}
