package de.tradingpulse.stage.tradingscreens.data;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static de.tradingpulse.stage.tradingscreens.data.EntrySignal.LONG_ENTRY;
import static de.tradingpulse.stage.tradingscreens.data.EntrySignal.SHORT_ENTRY;
import static de.tradingpulse.stage.tradingscreens.data.ExitSignal.LONG_EXIT;
import static de.tradingpulse.stage.tradingscreens.data.ExitSignal.LONG_WARNING;
import static de.tradingpulse.stage.tradingscreens.data.ExitSignal.SHORT_EXIT;
import static de.tradingpulse.stage.tradingscreens.data.ExitSignal.SHORT_WARNING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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
import de.tradingpulse.stage.systems.recordtypes.ImpulseData;

class SwingTradingScreenDataTest {

	@Test
	void test_serde() throws JsonProcessingException {
		SymbolTimestampKey	sTs	= SymbolTimestampKey.builder()
				.symbol("symbol")
				.timestamp(1)
				.build();
		ImpulseData	sID	= createImpulseData(NEUTRAL, LONG);
		sID.setKey(sTs);
		
		SymbolTimestampKey	lTs	= SymbolTimestampKey.builder()
				.symbol("symbol")
				.timestamp(0)
				.build();
		ImpulseData	lID	= createImpulseData(SHORT, NEUTRAL);
		lID.setKey(lTs);
		
		SwingTradingScreenData data = SwingTradingScreenData.builder()
				.key(sTs)
				.longRangeImpulseData(lID)
				.shortRangeImpulseData(sID)
				.build();
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(data);
		SwingTradingScreenData serdeData = mapper.readValue(json, SwingTradingScreenData.class);
		
		assertEquals(data, serdeData);
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
		SwingTradingScreenData data = SwingTradingScreenData.builder()
				.longRangeImpulseData(createImpulseData(longRangeImpulseLast, longRangeImpulseCurrent))
				.shortRangeImpulseData(createImpulseData(shortRangeImpulseLast, shortRangeImpulseCurrent))
				.build();
		
		assertEquals(last, data.getLastTradingDirection());
		assertEquals(current, data.getTradingDirection());
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
		SwingTradingScreenData data = SwingTradingScreenData.builder()
				.longRangeImpulseData(createImpulseData(longRangeImpulseLast, longRangeImpulseCurrent))
				.shortRangeImpulseData(createImpulseData(shortRangeImpulseLast, shortRangeImpulseCurrent))
				.build();
		
		if(entrySignal == null) {
			assertTrue(data.getEntrySignal().isEmpty());
		} else {
			assertEquals(entrySignal, data.getEntrySignal().get());
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
		SwingTradingScreenData data = SwingTradingScreenData.builder()
				.longRangeImpulseData(createImpulseData(longRangeImpulseLast, longRangeImpulseCurrent))
				.shortRangeImpulseData(createImpulseData(shortRangeImpulseLast, shortRangeImpulseCurrent))
				.build();
		
		try {
			data.getExitSignal();
			fail("UnsupportedOperationException expected");
		} catch (UnsupportedOperationException e) {
			// OK
		}
	}
	
	ImpulseData createImpulseData(TradingDirection last, TradingDirection current) {
		return ImpulseData.builder()
				.lastTradingDirection(last)
				.tradingDirection(current)
				.build();
	}
	
	static Stream<Arguments> swingTradingScreenData() {
		return Stream.of(
				//			weekly				daily				screen
				//			last	current		last	current		last	current	entry		exit
				arguments(	SHORT,	SHORT,		SHORT,	SHORT,		SHORT,	SHORT,	null,		null),			// 1
				arguments(	SHORT,	SHORT,		SHORT,	NEUTRAL,	SHORT,	SHORT,	null,		SHORT_WARNING),
				arguments(	SHORT,	SHORT,		SHORT,	LONG,		SHORT,	SHORT,	null,		SHORT_EXIT),
				
				arguments(	SHORT,	SHORT,		NEUTRAL,SHORT,		SHORT,	SHORT,	SHORT_ENTRY,null),			// 4
				arguments(	SHORT,	SHORT,		NEUTRAL,NEUTRAL,	SHORT,	SHORT,	null,		null),
				arguments(	SHORT,	SHORT,		NEUTRAL,LONG,		SHORT,	SHORT,	null,		SHORT_EXIT),	
				
				arguments(	SHORT,	SHORT,		LONG,	SHORT,		SHORT,	SHORT,	SHORT_ENTRY,null),			// 7
				arguments(	SHORT,	SHORT,		LONG,	NEUTRAL,	SHORT,	SHORT,	SHORT_ENTRY,null),
				arguments(	SHORT,	SHORT,		LONG,	LONG,		SHORT,	SHORT,	null,		null),
				
				arguments(	SHORT,	NEUTRAL,	SHORT,	SHORT,		SHORT,	NEUTRAL,null,		SHORT_WARNING),	// 10
				arguments(	SHORT,	NEUTRAL,	SHORT,	NEUTRAL,	SHORT,	NEUTRAL,LONG_ENTRY,	SHORT_WARNING),
				arguments(	SHORT,	NEUTRAL,	SHORT,	LONG,		SHORT,	NEUTRAL,LONG_ENTRY,	SHORT_EXIT),
				
				arguments(	SHORT,	NEUTRAL,	NEUTRAL,SHORT,		SHORT,	NEUTRAL,null,		null),			// 13
				arguments(	SHORT,	NEUTRAL,	NEUTRAL,NEUTRAL,	SHORT,	NEUTRAL,LONG_ENTRY,	SHORT_WARNING),
				arguments(	SHORT,	NEUTRAL,	NEUTRAL,LONG,		SHORT,	NEUTRAL,LONG_ENTRY,	SHORT_EXIT),
				
				arguments(	SHORT,	NEUTRAL,	LONG,	SHORT,		SHORT,	NEUTRAL,null,		null),			// 16
				arguments(	SHORT,	NEUTRAL,	LONG,	NEUTRAL,	SHORT,	NEUTRAL,null,		null),
				arguments(	SHORT,	NEUTRAL,	LONG,	LONG,		SHORT,	NEUTRAL,LONG_ENTRY,	null),
				
				arguments(	SHORT,	LONG,		SHORT,	SHORT,		SHORT,	LONG,	null,		SHORT_EXIT),	// 19
				arguments(	SHORT,	LONG,		SHORT,	NEUTRAL,	SHORT,	LONG,	LONG_ENTRY,	SHORT_EXIT),
				arguments(	SHORT,	LONG,		SHORT,	LONG,		SHORT,	LONG,	LONG_ENTRY,	SHORT_EXIT),
				
				arguments(	SHORT,	LONG,		NEUTRAL,SHORT,		SHORT,	LONG,	null,		null),
				arguments(	SHORT,	LONG,		NEUTRAL,NEUTRAL,	SHORT,	LONG,	LONG_ENTRY,	SHORT_EXIT),	// 22
				arguments(	SHORT,	LONG,		NEUTRAL,LONG,		SHORT,	LONG,	LONG_ENTRY,	SHORT_EXIT),
				
				arguments(	SHORT,	LONG,		LONG,	SHORT,		SHORT,	LONG,	null,		null),			// 25
				arguments(	SHORT,	LONG,		LONG,	NEUTRAL,	SHORT,	LONG,	null,		null),
				arguments(	SHORT,	LONG,		LONG,	LONG,		SHORT,	LONG,	LONG_ENTRY,	null),
				
				arguments(	NEUTRAL,SHORT,		SHORT,	SHORT,		NEUTRAL,SHORT,	SHORT_ENTRY,null),			// 28
				arguments(	NEUTRAL,SHORT,		SHORT,	NEUTRAL,	NEUTRAL,SHORT,	null,		null),
				arguments(	NEUTRAL,SHORT,		SHORT,	LONG,		NEUTRAL,SHORT,	null,		null),
				
				arguments(	NEUTRAL,SHORT,		NEUTRAL,SHORT,		NEUTRAL,SHORT,	SHORT_ENTRY,LONG_EXIT),		// 31
				arguments(	NEUTRAL,SHORT,		NEUTRAL,NEUTRAL,	NEUTRAL,SHORT,	SHORT_ENTRY,LONG_EXIT),
				arguments(	NEUTRAL,SHORT,		NEUTRAL,LONG,		NEUTRAL,SHORT,	null,		null),
				
				arguments(	NEUTRAL,SHORT,		LONG,	SHORT,		NEUTRAL,SHORT,	SHORT_ENTRY,LONG_EXIT),		// 34
				arguments(	NEUTRAL,SHORT,		LONG,	NEUTRAL,	NEUTRAL,SHORT,	SHORT_ENTRY,LONG_EXIT),
				arguments(	NEUTRAL,SHORT,		LONG,	LONG,		NEUTRAL,SHORT,	null,		LONG_EXIT),
				
				arguments(	NEUTRAL,NEUTRAL,	SHORT,	SHORT,		NEUTRAL,NEUTRAL,null,		null),			// 37
				arguments(	NEUTRAL,NEUTRAL,	SHORT,	NEUTRAL,	NEUTRAL,NEUTRAL,LONG_ENTRY,	SHORT_WARNING),
				arguments(	NEUTRAL,NEUTRAL,	SHORT,	LONG,		NEUTRAL,NEUTRAL,LONG_ENTRY,	SHORT_EXIT),
				
				arguments(	NEUTRAL,NEUTRAL,	NEUTRAL,SHORT,		NEUTRAL,NEUTRAL,SHORT_ENTRY,LONG_EXIT),		// 40
				arguments(	NEUTRAL,NEUTRAL,	NEUTRAL,NEUTRAL,	NEUTRAL,NEUTRAL,null,		null),
				arguments(	NEUTRAL,NEUTRAL,	NEUTRAL,LONG,		NEUTRAL,NEUTRAL,LONG_ENTRY,	SHORT_EXIT),
				
				arguments(	NEUTRAL,NEUTRAL,	LONG,	SHORT,		NEUTRAL,NEUTRAL,SHORT_ENTRY,LONG_EXIT),		// 43
				arguments(	NEUTRAL,NEUTRAL,	LONG,	NEUTRAL,	NEUTRAL,NEUTRAL,SHORT_ENTRY,LONG_WARNING),
				arguments(	NEUTRAL,NEUTRAL,	LONG,	LONG,		NEUTRAL,NEUTRAL,null,		null),
				
				arguments(	NEUTRAL,LONG,		SHORT,	SHORT,		NEUTRAL,LONG,	null,		SHORT_EXIT),	// 46
				arguments(	NEUTRAL,LONG,		SHORT,	NEUTRAL,	NEUTRAL,LONG,	LONG_ENTRY,	SHORT_EXIT),
				arguments(	NEUTRAL,LONG,		SHORT,	LONG,		NEUTRAL,LONG,	LONG_ENTRY,	SHORT_EXIT),
				
				arguments(	NEUTRAL,LONG,		NEUTRAL,SHORT,		NEUTRAL,LONG,	null,		null),			// 49
				arguments(	NEUTRAL,LONG,		NEUTRAL,NEUTRAL,	NEUTRAL,LONG,	LONG_ENTRY,	SHORT_EXIT),
				arguments(	NEUTRAL,LONG,		NEUTRAL,LONG,		NEUTRAL,LONG,	LONG_ENTRY,	SHORT_EXIT),
				
				arguments(	NEUTRAL,LONG,		LONG,	SHORT,		NEUTRAL,LONG,	null,		null),			// 52
				arguments(	NEUTRAL,LONG,		LONG,	NEUTRAL,	NEUTRAL,LONG,	null,		null),
				arguments(	NEUTRAL,LONG,		LONG,	LONG,		NEUTRAL,LONG,	LONG_ENTRY,	null),
				
				arguments(	LONG,	SHORT,		SHORT,	SHORT,		LONG,	SHORT,	SHORT_ENTRY,null),			// 55
				arguments(	LONG,	SHORT,		SHORT,	NEUTRAL,	LONG,	SHORT,	null,		null),
				arguments(	LONG,	SHORT,		SHORT,	LONG,		LONG,	SHORT,	null,		null),
				
				arguments(	LONG,	SHORT,		NEUTRAL,SHORT,		LONG,	SHORT,	SHORT_ENTRY,LONG_EXIT),		// 58
				arguments(	LONG,	SHORT,		NEUTRAL,NEUTRAL,	LONG,	SHORT,	SHORT_ENTRY,LONG_EXIT),
				arguments(	LONG,	SHORT,		NEUTRAL,LONG,		LONG,	SHORT,	null,		null),
				
				arguments(	LONG,	SHORT,		LONG,	SHORT,		LONG,	SHORT,	SHORT_ENTRY,LONG_EXIT),		// 61
				arguments(	LONG,	SHORT,		LONG,	NEUTRAL,	LONG,	SHORT,	SHORT_ENTRY,LONG_EXIT),
				arguments(	LONG,	SHORT,		LONG,	LONG,		LONG,	SHORT,	null,		LONG_EXIT),
				
				arguments(	LONG,	NEUTRAL,	SHORT,	SHORT,		LONG,	NEUTRAL,SHORT_ENTRY,null),			// 64
				arguments(	LONG,	NEUTRAL,	SHORT,	NEUTRAL,	LONG,	NEUTRAL,null,		null),
				arguments(	LONG,	NEUTRAL,	SHORT,	LONG,		LONG,	NEUTRAL,null,		null),
				
				arguments(	LONG,	NEUTRAL,	NEUTRAL,SHORT,		LONG,	NEUTRAL,SHORT_ENTRY,LONG_EXIT),		// 67
				arguments(	LONG,	NEUTRAL,	NEUTRAL,NEUTRAL,	LONG,	NEUTRAL,SHORT_ENTRY,LONG_WARNING),
				arguments(	LONG,	NEUTRAL,	NEUTRAL,LONG,		LONG,	NEUTRAL,null,		null),
				
				arguments(	LONG,	NEUTRAL,	LONG,	SHORT,		LONG,	NEUTRAL,SHORT_ENTRY,LONG_EXIT),		// 70
				arguments(	LONG,	NEUTRAL,	LONG,	NEUTRAL,	LONG,	NEUTRAL,SHORT_ENTRY,LONG_WARNING),
				arguments(	LONG,	NEUTRAL,	LONG,	LONG,		LONG,	NEUTRAL,null,		LONG_WARNING),
				
				arguments(	LONG,	LONG,		SHORT,	SHORT,		LONG,	LONG,	null,		null),			// 73
				arguments(	LONG,	LONG,		SHORT,	NEUTRAL,	LONG,	LONG,	LONG_ENTRY,	null),
				arguments(	LONG,	LONG,		SHORT,	LONG,		LONG,	LONG,	LONG_ENTRY,	null),
				
				arguments(	LONG,	LONG,		NEUTRAL,SHORT,		LONG,	LONG,	null,		LONG_EXIT),		// 76
				arguments(	LONG,	LONG,		NEUTRAL,NEUTRAL,	LONG,	LONG,	null,		null),
				arguments(	LONG,	LONG,		NEUTRAL,LONG,		LONG,	LONG,	LONG_ENTRY,	null),
				
				arguments(	LONG,	LONG,		LONG,	SHORT,		LONG,	LONG,	null,		LONG_EXIT),		// 79
				arguments(	LONG,	LONG,		LONG,	NEUTRAL,	LONG,	LONG,	null,		LONG_WARNING),
				arguments(	LONG,	LONG,		LONG,	LONG,		LONG,	LONG,	null,		null)
			);
	}
}
