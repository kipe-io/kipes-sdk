package de.tradingpulse.common.stream.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SymbolTimestampKeyTest {
	
	@Test
	void test_from_OHLCVRawData() {
		
		SymbolTimestampKey stk = SymbolTimestampKey.from(
				OHLCVDataRaw.builder()
				.symbol("symbol")
				.date("1970-01-02")
				.build());
		
		assertEquals("symbol", stk.getSymbol());
		assertEquals(86400000, stk.getTimestamp());
	}

}
