package de.tradingpulse.common.stream.aggregates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

class IncrementalAggregateTest {

	// ------------------------------------------------------------------------
	// test use case on initial
	// initial means there has been only one timestamp seen
	// ------------------------------------------------------------------------
	
	@Test
	void test_usecase__on_initial__getAggregate_on_initial_returns_null() {
		IncrementalAggregate<CString> a = new IncrementalAggregate<>();

		assertNull(a.getAggregate(0));
	}
	
	@Test
	void test_usecase__on_initial__getAggregate_with_earlier_ts_returns_null() {
		IncrementalAggregate<CString> a = new IncrementalAggregate<>();

		a.setAggregate(1, cs("aggregate ts 1"));
		
		assertNull(a.getAggregate(0));
	}
	
	@Test
	void test_usecase__on_initial__getAggregate_with_same_ts_returns_null() {
		IncrementalAggregate<CString> a = new IncrementalAggregate<>();

		a.setAggregate(0, cs("aggregate ts 0"));
		
		assertNull(a.getAggregate(0));
	}
	
	@Test
	void test_usecase__on_initial__getAggregate_with_later_ts_returns_stored() {
		IncrementalAggregate<CString> a = new IncrementalAggregate<>();

		a.setAggregate(0, cs("aggregate ts 0"));
		
		assertEquals(cs("aggregate ts 0"), a.getAggregate(1));
	}
	
	// ------------------------------------------------------------------------
	// test use case on evolved
	// evolved means there has been more that one timestamps seen
	// ------------------------------------------------------------------------
	
	@Test
	void test_usecase__on_evolved__getAggregate_with_2nd_ts_returns_1st_ts() {
		IncrementalAggregate<CString> a = new IncrementalAggregate<>();

		a.setAggregate(0, cs("aggregate ts 0"));
		a.setAggregate(1, cs("aggregate ts 1"));
		
		assertEquals(cs("aggregate ts 0"), a.getAggregate(1));
	}
	
	@Test
	void test_usecase__on_evolved__getAggregate_with_3rd_ts_returns_2nd_ts() {
		IncrementalAggregate<CString> a = new IncrementalAggregate<>();


		a.setAggregate(0, cs("aggregate ts 0"));
		a.setAggregate(1, cs("aggregate ts 1"));
		
		assertEquals(cs("aggregate ts 1"), a.getAggregate(2));
	}
	
	// ------------------------------------------------------------------------
	// test serde
	// ------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	@Test
	void test_serde() throws JsonProcessingException {
		IncrementalAggregate<CString> a = new IncrementalAggregate<>();
		
		a.setAggregate(0, cs("aggregate ts 0"));
		a.setAggregate(1, cs("aggregate ts 1"));
		
		assertNotNull(a.getCurrentTimestamp());
		assertNotNull(a.getCurrentAggregate());
		assertNotNull(a.getStableTimestamp());
		assertNotNull(a.getStableAggregate());
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(a);
		a = mapper.readValue(json, IncrementalAggregate.class);
	}
	
	// ------------------------------------------------------------------------
	// utilities
	// ------------------------------------------------------------------------

	private CString cs(String s) {
		return new CString(s);
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private final static class CString implements DeepCloneable<CString> {
		private String s;
		
		public CString deepClone() {
			return new CString(this.s);
		}
	}
}
