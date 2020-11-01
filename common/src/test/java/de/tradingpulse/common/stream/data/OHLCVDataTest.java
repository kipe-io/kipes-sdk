package de.tradingpulse.common.stream.data;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import de.tradingpulse.common.stream.rawtypes.OHLCVRawRecord;
import de.tradingpulse.common.stream.recordtypes.OHLCVData;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;

class OHLCVDataTest {

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
		
		OHLCVData data = OHLCVData.from(rawData);
		
		assertEquals(rawData.getSymbol(), data.getKey().getSymbol());
		assertEquals(86400000, data.getKey().getTimestamp());
		assertEquals(rawData.getOpen(), data.getOpen());
		assertEquals(rawData.getHigh(), data.getHigh());
		assertEquals(rawData.getLow(), data.getLow());
		assertEquals(rawData.getClose(), data.getClose());
		assertEquals(rawData.getVolume(), data.getVolume());
		
	}
	
	@Test
	void test_aggregateWith() {
		OHLCVData a = OHLCVData.builder()
				.key(SymbolTimestampKey.builder()
						.symbol("symbol")
						.timestamp(0)
						.build())
				.open(0d)
				.high(2d)
				.low(4d)
				.close(6d)
				.volume(8l)
				.build();
		
		OHLCVData b = OHLCVData.builder()
				.key(SymbolTimestampKey.builder()
						.symbol("symbol")
						.timestamp(1)
						.build())
				.open(1d)
				.high(3d)
				.low(5d)
				.close(7d)
				.volume(9l)
				.build();
		
		OHLCVData aggregate = a.aggregateWith(b);
		
		assertEquals(a.getKey(), aggregate.getKey());
		assertEquals(a.getOpen(), aggregate.getOpen());
		assertEquals(b.getHigh(), aggregate.getHigh());
		assertEquals(a.getLow(), aggregate.getLow());
		assertEquals(b.getClose(), aggregate.getClose());
		assertEquals(17l, aggregate.getVolume().longValue());
	}

}
