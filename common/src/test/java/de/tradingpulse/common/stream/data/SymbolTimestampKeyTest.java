package de.tradingpulse.common.stream.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import de.tradingpulse.common.stream.rawtypes.OHLCVRawRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;

class SymbolTimestampKeyTest {
	
	@Test
	void test_from_OHLCVRawData() {
		
		SymbolTimestampKey stk = SymbolTimestampKey.from(
				OHLCVRawRecord.builder()
				.symbol("symbol")
				.date("1970-01-02")
				.build());
		
		assertEquals("symbol", stk.getSymbol());
		assertEquals(86400000, stk.getTimestamp());
	}

}
