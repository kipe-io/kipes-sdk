package de.tradingpulse.stages.indicators.aggregates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stages.indicators.recordtypes.SSTOCRecord;

class SSTOCAggregateTest {

	@Test
	void test_aggregate() {
		SSTOCAggregate a = new SSTOCAggregate(3, 2, 2);
		
		assertNull(a.aggregate(createRecord(2.0, 1.0, 1.5)));			// 1
		assertNull(a.aggregate(createRecord(3.0, 2.0, 2.5)));			// 2
		assertNull(a.aggregate(createRecord(4.0, 3.0, 3.5)));			// 3
		assertNull(a.aggregate(createRecord(5.0, 4.0, 4.5)));			// 4
		
		SSTOCRecord record = a.aggregate(createRecord(6.0, 5.0, 5.5));	// 5
		assertNotNull(record);
		assertNotNull(record.getFast());
		assertNotNull(record.getSlow());
		assertNotNull(record.getFChange());
		assertNull(record.getSChange());
		
		record = a.aggregate(createRecord(7.0, 6.0, 6.5));				// 6
		assertNotNull(record);
		assertNotNull(record.getFast());
		assertNotNull(record.getSlow());
		assertNotNull(record.getFChange());
		assertNotNull(record.getSChange());
		
	}

	@Test
	void test_serde() throws JsonProcessingException {
		SSTOCAggregate a = new SSTOCAggregate(3, 2, 2);
		a.aggregate(createRecord(2.0, 1.0, 1.5));
		a.aggregate(createRecord(3.0, 2.0, 2.5));
		a.aggregate(createRecord(4.0, 3.0, 3.5));
		a.aggregate(createRecord(5.0, 4.0, 4.5));
		a.aggregate(createRecord(6.0, 5.0, 5.5));
		a.aggregate(createRecord(7.0, 6.0, 6.5));
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(a);
		SSTOCAggregate b = mapper.readValue(json, SSTOCAggregate.class);
		
		assertEquals(a.getN(), b.getN());
		assertEquals(
				Arrays.asList(a.getInputQueue().toArray()), 
				Arrays.asList(b.getInputQueue().toArray())); // ArrayBlockingQueue has no equals 
		
		assertEquals(a.getP1(), b.getP1());
		assertEquals(
				Arrays.asList(a.getRawStochasticQueue().toArray()), 
				Arrays.asList(b.getRawStochasticQueue().toArray())); // ArrayBlockingQueue has no equals 
		
		assertEquals(a.getP2(), b.getP2());
		assertEquals(
				Arrays.asList(a.getFastStochasticQueue().toArray()), 
				Arrays.asList(b.getFastStochasticQueue().toArray())); // ArrayBlockingQueue has no equals 
		
		assertEquals(a.getFastStochastic(), b.getFastStochastic());
		assertEquals(a.getSlowStochastic(), b.getSlowStochastic());
		
	}
	
	private OHLCVRecord createRecord(double high, double low, double close) {
		return OHLCVRecord.builder()
				.key(new SymbolTimestampKey("symbol", 0L))
				.high(high)
				.low(low)
				.close(close)
				.build();
	}
}
