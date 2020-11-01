package de.tradingpulse.common.stream.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

class AbstractIncrementalAggregateRecordTest {

	// ------------------------------------------------------------------------
	// test_getTimeRangeTimestamp
	// ------------------------------------------------------------------------
	
	@Test
	void test_getTimeRangeTimestamp__when_key_is_null_throw_ISE() {
		IncrementalAggregateRecord r = new IncrementalAggregateRecord();
		
		assertThrows(IllegalStateException.class, () -> r.getTimeRangeTimestamp());
	}

	@Test
	void test_getTimeRangeTimestamp__when_timerange_is_null_throw_ISE() {
		IncrementalAggregateRecord r = new IncrementalAggregateRecord();
		r.setTimeRange(null);
		
		assertThrows(IllegalStateException.class, () -> r.getTimeRangeTimestamp());
	}
	
	@Test
	void test_getTimeRangeTimestamp__calculates_the_correct_timestamp() {
		IncrementalAggregateRecord r = IncrementalAggregateRecord.builder()
				.key(SymbolTimestampKey.builder()
						.timestamp(0)
						.build())
				.build();
		
		assertEquals(0, r.getTimeRangeTimestamp());
	}
	
	// ------------------------------------------------------------------------
	// test json serialization/deserialization
	// ------------------------------------------------------------------------
	
	@Test
	void test_serde() throws JsonProcessingException {
		IncrementalAggregateRecord r = IncrementalAggregateRecord.builder()
				.key(SymbolTimestampKey.builder()
						.timestamp(1)
						.symbol("symbol")
						.build())
				.timeRange(TimeRange.DAY)
				.value("value")
				.build();
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(r);
		
		assertTrue(json.contains("timeRangeTimestamp"));
		
		IncrementalAggregateRecord deser = mapper.readValue(json, IncrementalAggregateRecord.class);
		assertEquals(r, deser);
	}
	
	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------
	
	@Data
	@EqualsAndHashCode(callSuper = true)
	@NoArgsConstructor
	@AllArgsConstructor
	@SuperBuilder
	private static class IncrementalAggregateRecord extends AbstractIncrementalAggregateRecord {
		private String value;
	}
}
