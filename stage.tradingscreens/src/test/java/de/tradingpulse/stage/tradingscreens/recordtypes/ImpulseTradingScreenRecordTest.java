package de.tradingpulse.stage.tradingscreens.recordtypes;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static de.tradingpulse.stage.tradingscreens.recordtypes.EntrySignal.ENTRY_LONG_MOMENTUM;
import static de.tradingpulse.stage.tradingscreens.recordtypes.EntrySignal.ENTRY_LONG_POTENTIAL;
import static de.tradingpulse.stage.tradingscreens.recordtypes.EntrySignal.ENTRY_SHORT_MOMENTUM;
import static de.tradingpulse.stage.tradingscreens.recordtypes.EntrySignal.ENTRY_SHORT_POTENTIAL;
import static de.tradingpulse.stage.tradingscreens.recordtypes.ExitSignal.EXIT_LONG;
import static de.tradingpulse.stage.tradingscreens.recordtypes.ExitSignal.EXIT_LONG_MOMENTUM;
import static de.tradingpulse.stage.tradingscreens.recordtypes.ExitSignal.EXIT_SHORT;
import static de.tradingpulse.stage.tradingscreens.recordtypes.ExitSignal.EXIT_SHORT_MOMENTUM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;

class ImpulseTradingScreenRecordTest {

	@Test
	void test_serde() throws JsonProcessingException {
		SymbolTimestampKey	sTs	= SymbolTimestampKey.builder()
				.symbol("symbol")
				.timestamp(1)
				.build();
		ImpulseRecord	sID	= createImpulseData(NEUTRAL, LONG);
		sID.setKey(sTs);
		
		SymbolTimestampKey	lTs	= SymbolTimestampKey.builder()
				.symbol("symbol")
				.timestamp(0)
				.build();
		ImpulseRecord	lID	= createImpulseData(SHORT, NEUTRAL);
		lID.setKey(lTs);
		
		ImpulseTradingScreenRecord record = ImpulseTradingScreenRecord.builder()
				.key(sTs)
				.longRangeImpulseRecord(lID)
				.shortRangeImpulseRecord(sID)
				.build();
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(record);
		ImpulseTradingScreenRecord serdeData = mapper.readValue(json, ImpulseTradingScreenRecord.class);
		
		assertEquals(record, serdeData);
	}
	
	@ParameterizedTest
	@MethodSource("swingTradingScreenData")
	void test_tradingDirection(
			TradingDirection longRangeImpulseLast, 
			TradingDirection longRangeImpulseCurrent, 
			TradingDirection shortRangeImpulseLast, 
			TradingDirection shortRangeImpulseCurrent, 
			TradingDirection last, 
			TradingDirection current,
			EntrySignal entrySignal,
			ExitSignal exitSignal
	) {
		ImpulseTradingScreenRecord record = ImpulseTradingScreenRecord.builder()
				.longRangeImpulseRecord(createImpulseData(longRangeImpulseLast, longRangeImpulseCurrent))
				.shortRangeImpulseRecord(createImpulseData(shortRangeImpulseLast, shortRangeImpulseCurrent))
				.build();
		
		assertEquals(last, record.getLastTradingDirection());
		assertEquals(current, record.getTradingDirection());
	}

	@ParameterizedTest
	@MethodSource("swingTradingScreenData")
	void test_getEntrySignal(
			TradingDirection longRangeImpulseLast, 
			TradingDirection longRangeImpulseCurrent, 
			TradingDirection shortRangeImpulseLast, 
			TradingDirection shortRangeImpulseCurrent, 
			TradingDirection last, 
			TradingDirection current,
			EntrySignal entrySignal,
			ExitSignal exitSignal
	) {
		ImpulseTradingScreenRecord record = ImpulseTradingScreenRecord.builder()
				.longRangeImpulseRecord(createImpulseData(longRangeImpulseLast, longRangeImpulseCurrent))
				.shortRangeImpulseRecord(createImpulseData(shortRangeImpulseLast, shortRangeImpulseCurrent))
				.build();
		
		if(entrySignal == null) {
			assertTrue(record.getEntrySignal().isEmpty());
		} else {
			assertEquals(entrySignal, record.getEntrySignal().get());
		}
	}

	
	@ParameterizedTest
	@MethodSource("swingTradingScreenData")
	void test_getExitSignal(
			TradingDirection longRangeImpulseLast, 
			TradingDirection longRangeImpulseCurrent, 
			TradingDirection shortRangeImpulseLast, 
			TradingDirection shortRangeImpulseCurrent, 
			TradingDirection last, 
			TradingDirection current,
			EntrySignal entrySignal,
			ExitSignal exitSignal
	) {
		ImpulseTradingScreenRecord record = ImpulseTradingScreenRecord.builder()
				.longRangeImpulseRecord(createImpulseData(longRangeImpulseLast, longRangeImpulseCurrent))
				.shortRangeImpulseRecord(createImpulseData(shortRangeImpulseLast, shortRangeImpulseCurrent))
				.build();
		
		if(exitSignal == null) {
			assertTrue(record.getExitSignal().isEmpty());
		} else {
			assertEquals(exitSignal, record.getExitSignal().get());
		}
	}
	
	ImpulseRecord createImpulseData(TradingDirection last, TradingDirection current) {
		return ImpulseRecord.builder()
				.lastTradingDirection(last)
				.tradingDirection(current)
				.build();
	}
	
	static Stream<Arguments> swingTradingScreenData() {
		return Stream.of(
				//			weekly				daily				screen
				//			last	current		last	current		last	current		entry					exit
				arguments(	SHORT,	SHORT,		SHORT,	SHORT,		SHORT,	SHORT,		null,					null),					// 1
				arguments(	SHORT,	SHORT,		SHORT,	NEUTRAL,	SHORT,	SHORT,		null,					EXIT_SHORT_MOMENTUM),
				arguments(	SHORT,	SHORT,		SHORT,	LONG,		SHORT,	SHORT,		null,					EXIT_SHORT),
				
				arguments(	SHORT,	SHORT,		NEUTRAL,SHORT,		SHORT,	SHORT,		ENTRY_SHORT_MOMENTUM,	null),					// 4
				arguments(	SHORT,	SHORT,		NEUTRAL,NEUTRAL,	SHORT,	SHORT,		null,					null),
				arguments(	SHORT,	SHORT,		NEUTRAL,LONG,		SHORT,	SHORT,		null,					EXIT_SHORT),	
				
				arguments(	SHORT,	SHORT,		LONG,	SHORT,		SHORT,	SHORT,		ENTRY_SHORT_MOMENTUM,	null),					// 7
				arguments(	SHORT,	SHORT,		LONG,	NEUTRAL,	SHORT,	SHORT,		ENTRY_SHORT_POTENTIAL,	null),
				arguments(	SHORT,	SHORT,		LONG,	LONG,		SHORT,	SHORT,		null,					null),
				
				arguments(	SHORT,	NEUTRAL,	SHORT,	SHORT,		SHORT,	NEUTRAL,	null,					EXIT_SHORT_MOMENTUM),	// 10
				arguments(	SHORT,	NEUTRAL,	SHORT,	NEUTRAL,	SHORT,	NEUTRAL,	ENTRY_LONG_POTENTIAL,	EXIT_SHORT_MOMENTUM),
				arguments(	SHORT,	NEUTRAL,	SHORT,	LONG,		SHORT,	NEUTRAL,	ENTRY_LONG_POTENTIAL,	EXIT_SHORT),
				
				arguments(	SHORT,	NEUTRAL,	NEUTRAL,SHORT,		SHORT,	NEUTRAL,	null,					null),					// 13
				arguments(	SHORT,	NEUTRAL,	NEUTRAL,NEUTRAL,	SHORT,	NEUTRAL,	ENTRY_LONG_POTENTIAL,	null),
				arguments(	SHORT,	NEUTRAL,	NEUTRAL,LONG,		SHORT,	NEUTRAL,	ENTRY_LONG_POTENTIAL,	EXIT_SHORT),
				
				arguments(	SHORT,	NEUTRAL,	LONG,	SHORT,		SHORT,	NEUTRAL,	null,					null),					// 16
				arguments(	SHORT,	NEUTRAL,	LONG,	NEUTRAL,	SHORT,	NEUTRAL,	null,					null),
				arguments(	SHORT,	NEUTRAL,	LONG,	LONG,		SHORT,	NEUTRAL,	ENTRY_LONG_POTENTIAL,	null),
				
				arguments(	SHORT,	LONG,		SHORT,	SHORT,		SHORT,	LONG,		null,					EXIT_SHORT),			// 19
				arguments(	SHORT,	LONG,		SHORT,	NEUTRAL,	SHORT,	LONG,		ENTRY_LONG_POTENTIAL,	EXIT_SHORT),
				arguments(	SHORT,	LONG,		SHORT,	LONG,		SHORT,	LONG,		ENTRY_LONG_MOMENTUM,	EXIT_SHORT),
				
				arguments(	SHORT,	LONG,		NEUTRAL,SHORT,		SHORT,	LONG,		null,					null),					// 22
				arguments(	SHORT,	LONG,		NEUTRAL,NEUTRAL,	SHORT,	LONG,		ENTRY_LONG_POTENTIAL,	EXIT_SHORT),
				arguments(	SHORT,	LONG,		NEUTRAL,LONG,		SHORT,	LONG,		ENTRY_LONG_MOMENTUM,	EXIT_SHORT),
				
				arguments(	SHORT,	LONG,		LONG,	SHORT,		SHORT,	LONG,		null,					null),					// 25
				arguments(	SHORT,	LONG,		LONG,	NEUTRAL,	SHORT,	LONG,		null,					null),
				arguments(	SHORT,	LONG,		LONG,	LONG,		SHORT,	LONG,		ENTRY_LONG_MOMENTUM,	null),
				
				arguments(	NEUTRAL,SHORT,		SHORT,	SHORT,		NEUTRAL,SHORT,		ENTRY_SHORT_MOMENTUM,	null),					// 28
				arguments(	NEUTRAL,SHORT,		SHORT,	NEUTRAL,	NEUTRAL,SHORT,		null,					null),
				arguments(	NEUTRAL,SHORT,		SHORT,	LONG,		NEUTRAL,SHORT,		null,					null),
				
				arguments(	NEUTRAL,SHORT,		NEUTRAL,SHORT,		NEUTRAL,SHORT,		ENTRY_SHORT_MOMENTUM,	EXIT_LONG),				// 31
				arguments(	NEUTRAL,SHORT,		NEUTRAL,NEUTRAL,	NEUTRAL,SHORT,		ENTRY_SHORT_POTENTIAL,	EXIT_LONG),
				arguments(	NEUTRAL,SHORT,		NEUTRAL,LONG,		NEUTRAL,SHORT,		null,					null),
				
				arguments(	NEUTRAL,SHORT,		LONG,	SHORT,		NEUTRAL,SHORT,		ENTRY_SHORT_MOMENTUM,	EXIT_LONG),				// 34
				arguments(	NEUTRAL,SHORT,		LONG,	NEUTRAL,	NEUTRAL,SHORT,		ENTRY_SHORT_POTENTIAL,	EXIT_LONG),
				arguments(	NEUTRAL,SHORT,		LONG,	LONG,		NEUTRAL,SHORT,		null,					EXIT_LONG),
				
				arguments(	NEUTRAL,NEUTRAL,	SHORT,	SHORT,		NEUTRAL,NEUTRAL,	null,					null),					// 37
				arguments(	NEUTRAL,NEUTRAL,	SHORT,	NEUTRAL,	NEUTRAL,NEUTRAL,	ENTRY_LONG_POTENTIAL,	null),
				arguments(	NEUTRAL,NEUTRAL,	SHORT,	LONG,		NEUTRAL,NEUTRAL,	ENTRY_LONG_POTENTIAL,	EXIT_SHORT),
				
				arguments(	NEUTRAL,NEUTRAL,	NEUTRAL,SHORT,		NEUTRAL,NEUTRAL,	ENTRY_SHORT_POTENTIAL,	EXIT_LONG),				// 40
				arguments(	NEUTRAL,NEUTRAL,	NEUTRAL,NEUTRAL,	NEUTRAL,NEUTRAL,	null,					null),
				arguments(	NEUTRAL,NEUTRAL,	NEUTRAL,LONG,		NEUTRAL,NEUTRAL,	ENTRY_LONG_POTENTIAL,	EXIT_SHORT),
				
				arguments(	NEUTRAL,NEUTRAL,	LONG,	SHORT,		NEUTRAL,NEUTRAL,	ENTRY_SHORT_POTENTIAL,	EXIT_LONG),				// 43
				arguments(	NEUTRAL,NEUTRAL,	LONG,	NEUTRAL,	NEUTRAL,NEUTRAL,	ENTRY_SHORT_POTENTIAL,	null),
				arguments(	NEUTRAL,NEUTRAL,	LONG,	LONG,		NEUTRAL,NEUTRAL,	null,					null),
				
				arguments(	NEUTRAL,LONG,		SHORT,	SHORT,		NEUTRAL,LONG,		null,					EXIT_SHORT),			// 46
				arguments(	NEUTRAL,LONG,		SHORT,	NEUTRAL,	NEUTRAL,LONG,		ENTRY_LONG_POTENTIAL,	EXIT_SHORT),
				arguments(	NEUTRAL,LONG,		SHORT,	LONG,		NEUTRAL,LONG,		ENTRY_LONG_MOMENTUM,	EXIT_SHORT),
				
				arguments(	NEUTRAL,LONG,		NEUTRAL,SHORT,		NEUTRAL,LONG,		null,					null),					// 49
				arguments(	NEUTRAL,LONG,		NEUTRAL,NEUTRAL,	NEUTRAL,LONG,		ENTRY_LONG_POTENTIAL,	EXIT_SHORT),
				arguments(	NEUTRAL,LONG,		NEUTRAL,LONG,		NEUTRAL,LONG,		ENTRY_LONG_MOMENTUM,	EXIT_SHORT),
				
				arguments(	NEUTRAL,LONG,		LONG,	SHORT,		NEUTRAL,LONG,		null,					null),					// 52
				arguments(	NEUTRAL,LONG,		LONG,	NEUTRAL,	NEUTRAL,LONG,		null,					null),
				arguments(	NEUTRAL,LONG,		LONG,	LONG,		NEUTRAL,LONG,		ENTRY_LONG_MOMENTUM,	null),
				
				arguments(	LONG,	SHORT,		SHORT,	SHORT,		LONG,	SHORT,		ENTRY_SHORT_MOMENTUM,	null),					// 55
				arguments(	LONG,	SHORT,		SHORT,	NEUTRAL,	LONG,	SHORT,		null,					null),
				arguments(	LONG,	SHORT,		SHORT,	LONG,		LONG,	SHORT,		null,					null),
				
				arguments(	LONG,	SHORT,		NEUTRAL,SHORT,		LONG,	SHORT,		ENTRY_SHORT_MOMENTUM,	EXIT_LONG),				// 58
				arguments(	LONG,	SHORT,		NEUTRAL,NEUTRAL,	LONG,	SHORT,		ENTRY_SHORT_POTENTIAL,	EXIT_LONG),
				arguments(	LONG,	SHORT,		NEUTRAL,LONG,		LONG,	SHORT,		null,					null),
				
				arguments(	LONG,	SHORT,		LONG,	SHORT,		LONG,	SHORT,		ENTRY_SHORT_MOMENTUM,	EXIT_LONG),				// 61
				arguments(	LONG,	SHORT,		LONG,	NEUTRAL,	LONG,	SHORT,		ENTRY_SHORT_POTENTIAL,	EXIT_LONG),
				arguments(	LONG,	SHORT,		LONG,	LONG,		LONG,	SHORT,		null,					EXIT_LONG),
				
				arguments(	LONG,	NEUTRAL,	SHORT,	SHORT,		LONG,	NEUTRAL,	ENTRY_SHORT_POTENTIAL,	null),					// 64
				arguments(	LONG,	NEUTRAL,	SHORT,	NEUTRAL,	LONG,	NEUTRAL,	null,					null),
				arguments(	LONG,	NEUTRAL,	SHORT,	LONG,		LONG,	NEUTRAL,	null,					null),
				
				arguments(	LONG,	NEUTRAL,	NEUTRAL,SHORT,		LONG,	NEUTRAL,	ENTRY_SHORT_POTENTIAL,	EXIT_LONG),				// 67
				arguments(	LONG,	NEUTRAL,	NEUTRAL,NEUTRAL,	LONG,	NEUTRAL,	ENTRY_SHORT_POTENTIAL,	null),
				arguments(	LONG,	NEUTRAL,	NEUTRAL,LONG,		LONG,	NEUTRAL,	null,					null),
				
				arguments(	LONG,	NEUTRAL,	LONG,	SHORT,		LONG,	NEUTRAL,	ENTRY_SHORT_POTENTIAL,	EXIT_LONG),				// 70
				arguments(	LONG,	NEUTRAL,	LONG,	NEUTRAL,	LONG,	NEUTRAL,	ENTRY_SHORT_POTENTIAL,	EXIT_LONG_MOMENTUM),
				arguments(	LONG,	NEUTRAL,	LONG,	LONG,		LONG,	NEUTRAL,	null,					EXIT_LONG_MOMENTUM),
				
				arguments(	LONG,	LONG,		SHORT,	SHORT,		LONG,	LONG,		null,					null),					// 73
				arguments(	LONG,	LONG,		SHORT,	NEUTRAL,	LONG,	LONG,		ENTRY_LONG_POTENTIAL,	null),
				arguments(	LONG,	LONG,		SHORT,	LONG,		LONG,	LONG,		ENTRY_LONG_MOMENTUM,	null),
				
				arguments(	LONG,	LONG,		NEUTRAL,SHORT,		LONG,	LONG,		null,					EXIT_LONG),				// 76
				arguments(	LONG,	LONG,		NEUTRAL,NEUTRAL,	LONG,	LONG,		null,					null),
				arguments(	LONG,	LONG,		NEUTRAL,LONG,		LONG,	LONG,		ENTRY_LONG_MOMENTUM,	null),
				
				arguments(	LONG,	LONG,		LONG,	SHORT,		LONG,	LONG,		null,					EXIT_LONG),				// 79
				arguments(	LONG,	LONG,		LONG,	NEUTRAL,	LONG,	LONG,		null,					EXIT_LONG_MOMENTUM),
				arguments(	LONG,	LONG,		LONG,	LONG,		LONG,	LONG,		null,					null)
			);
	}
}
