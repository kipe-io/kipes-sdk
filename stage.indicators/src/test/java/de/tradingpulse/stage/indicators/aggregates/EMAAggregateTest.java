package de.tradingpulse.stage.indicators.aggregates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.tradingpulse.stages.indicators.aggregates.EMAAggregate;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;

class EMAAggregateTest {

	// ---- n = 1 -------------------------------------------------------------
	
	@Test
	void test_aggregate__n_1__first_aggregate_is_value() {
		EMAAggregate a = new EMAAggregate(1);

		double k = 2.0 / ( 1 + 1.0 );
		
		double initialEma = 25.2 / 1.0; // avg
		
		double expected = initialEma * k;
		
		assertEquals(expected, a.aggregate(25.2).getValue());
	}

	@Test
	void test_aggregate__n_1__second_aggregate_is_value() {
		EMAAggregate a = new EMAAggregate(1);

		double k = 2.0 / ( 1 + 1.0 );
		
		double initialEma = 1.0 / 1.0; // avg
		
		double expected = 25.2 * k + initialEma * (1-k);
		
		a.aggregate(1.0);
		assertEquals(expected, a.aggregate(25.2).getValue());
	}

	// ---- n = 2 -------------------------------------------------------------
	
	@Test
	void test_aggregate__n_2__first_aggregate_is_null() {
		EMAAggregate a = new EMAAggregate(2);
		
		assertNull(a.aggregate(25.2));
	}

	@Test
	void test_aggregate__n_2__second_aggregate_is_avg() {
		EMAAggregate a = new EMAAggregate(2);

		double initialEma = (1.0 + 25.2) / 2.0; // avg
		
		double expected = initialEma;
		
		a.aggregate(1.0);
		assertEquals(expected, a.aggregate(25.2).getValue());
	}
	
	@Test
	void test_aggregate__n_2__third_aggregate_is_ema() {
		EMAAggregate a = new EMAAggregate(2);

		double k = 2.0 / ( 2 + 1.0 );
		
		double initialEma = (1.0 + 25.2) / 2.0; // avg
		
		double expected = 3.0 * k + initialEma * (1-k);
		
		a.aggregate(1.0);
		a.aggregate(25.2);		
		assertEquals(expected, a.aggregate(3.0).getValue());
	}
	
	// ---- n = 10 ------------------------------------------------------------
	
	@Test
	void test_aggregate__n_10__nineth_aggregate_is_null() {
		EMAAggregate a = new EMAAggregate(10);

		a.aggregate(1.0);
		a.aggregate(2.0);		
		a.aggregate(3.0);		
		a.aggregate(4.0);		
		a.aggregate(5.0);		
		a.aggregate(6.0);		
		a.aggregate(7.0);		
		a.aggregate(8.0);		
		assertNull( a.aggregate(9.0));		
	}
	
	@Test
	void test_aggregate__n_10__tenth_aggregate_is_sma() {
		EMAAggregate a = new EMAAggregate(10);

		double initialEma = (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10.0; // avg
		
		a.aggregate(1.0);
		a.aggregate(2.0);
		a.aggregate(3.0);
		a.aggregate(4.0);
		a.aggregate(5.0);
		a.aggregate(6.0);
		a.aggregate(7.0);
		a.aggregate(8.0);
		a.aggregate(9.0);
		
		assertEquals(initialEma, a.aggregate(10.0).getValue());		
	}
	
	@Test
	void test_aggregate__n_10__eleventh_aggregate_is_ema_and_changes_are_correct() {
		EMAAggregate a = new EMAAggregate(10);

		double k = 2.0 / ( 10 + 1.0 );
		
		double initialEma = (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10.0; // avg
		
		a.aggregate(1.0);
		a.aggregate(2.0);
		a.aggregate(3.0);
		a.aggregate(4.0);
		a.aggregate(5.0);
		a.aggregate(6.0);
		a.aggregate(7.0);
		a.aggregate(8.0);
		a.aggregate(9.0);
		DoubleRecord ema_10 = a.aggregate(10.0);
		
		double expected = 11.0 * k + initialEma * (1-k);
		
		DoubleRecord ema_11 = a.aggregate(11.0);
		assertEquals(expected, ema_11.getValue());
		
		assertEquals(expected - ema_10.getValue(), ema_11.getVChange());
	}
	
	@Test
	void test_aggregate__n_10__twelfth_aggregate_is_ema_after_serde()
			throws JsonProcessingException
	{
		EMAAggregate a = new EMAAggregate(10);

		double k = 2.0 / ( 10 + 1.0 );
		
		a.aggregate(1.0);
		a.aggregate(2.0);
		a.aggregate(3.0);
		a.aggregate(4.0);
		a.aggregate(5.0);
		a.aggregate(6.0);
		a.aggregate(7.0);
		a.aggregate(8.0);
		a.aggregate(9.0);
		a.aggregate(10.0);
		DoubleRecord ema_11 = a.aggregate(11.0);

		double expected = 12.0 * k + ema_11.getValue() * (1-k);
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(a);
		a = mapper.readValue(json, EMAAggregate.class);
		
		DoubleRecord ema_12 = a.aggregate(12.0);
		assertEquals(expected, ema_12.getValue());
		assertEquals(expected - ema_11.getValue(), ema_12.getVChange());
	}
}
