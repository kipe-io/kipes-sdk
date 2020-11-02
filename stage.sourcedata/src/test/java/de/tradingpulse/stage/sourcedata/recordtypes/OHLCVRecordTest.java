package de.tradingpulse.stage.sourcedata.recordtypes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TimeRange;

class OHLCVRecordTest {

	
	@Test
	void test_extractKey() {
		
		SymbolTimestampKey stk = OHLCVRecord.extractKey(
				OHLCVRawRecord.builder()
				.symbol("symbol")
				.date("1970-01-02")
				.build());
		
		assertEquals("symbol", stk.getSymbol());
		assertEquals(86400000, stk.getTimestamp());
	}
	
	@Test
	void test_from() {
		OHLCVRawRecord rawData = OHLCVRawRecord.builder()
				.symbol("symbol")
				.date("1970-01-02")
				.open(1.0)
				.high(2.0)
				.low(3.0)
				.close(4.0)
				.volume(5L)
				.build();
		
		OHLCVRecord data = OHLCVRecord.from(rawData);
		
		assertEquals(rawData.getSymbol(), data.getKey().getSymbol());
		assertEquals(86400000, data.getKey().getTimestamp());
		assertEquals(TimeRange.DAY, data.getTimeRange());
		assertEquals(86400000, data.getTimeRangeTimestamp());
		assertEquals(rawData.getOpen(), data.getOpen());
		assertEquals(rawData.getHigh(), data.getHigh());
		assertEquals(rawData.getLow(), data.getLow());
		assertEquals(rawData.getClose(), data.getClose());
		assertEquals(rawData.getVolume(), data.getVolume());
		
	}
	
	@Test
	void test_aggregateWith() {
		OHLCVRecord a = OHLCVRecord.builder()
				.key(SymbolTimestampKey.builder()
						.symbol("symbol")
						.timestamp(0)
						.build())
				.timeRange(TimeRange.DAY)
				.open(0d)
				.high(2d)
				.low(4d)
				.close(6d)
				.volume(8l)
				.build();
		
		OHLCVRecord b = OHLCVRecord.builder()
				.key(SymbolTimestampKey.builder()
						.symbol("symbol")
						.timestamp(1)
						.build())
				.timeRange(TimeRange.MINUTE)
				.open(1d)
				.high(3d)
				.low(5d)
				.close(7d)
				.volume(9l)
				.build();
		
		OHLCVRecord aggregate = a.aggregateWith(b);
		
		assertEquals(b.getKey(), aggregate.getKey());
		assertEquals(TimeRange.DAY, aggregate.getTimeRange());
		assertEquals(a.getOpen(), aggregate.getOpen());
		assertEquals(b.getHigh(), aggregate.getHigh());
		assertEquals(a.getLow(), aggregate.getLow());
		assertEquals(b.getClose(), aggregate.getClose());
		assertEquals(17l, aggregate.getVolume().longValue());
	}

}
