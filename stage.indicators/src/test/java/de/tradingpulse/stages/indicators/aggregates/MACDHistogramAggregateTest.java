package de.tradingpulse.stages.indicators.aggregates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.tradingpulse.stages.indicators.aggregates.EMAAggregate;
import de.tradingpulse.stages.indicators.aggregates.MACDHistogramAggregate;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;

class MACDHistogramAggregateTest {

	@Test
	void test_aggregate__returns_null_until_signal_has_values() {
		int fastPeriod = 2;
		int slowPeriod = 4;
		int signalPeriod = 2;
		
		MACDHistogramAggregate a = new MACDHistogramAggregate(fastPeriod, slowPeriod, signalPeriod);
		int count = 0;
		
		while(count < (slowPeriod + signalPeriod)-2) {
			assertNull(a.aggregate(2.35));
			count++;
		}
		
		assertNotNull(a.aggregate(2.35));
	}
	
	@Test
	void test_aggregate__calculates_correctly_also_with_serde()
	throws JsonProcessingException
	{
		int fastPeriod = 2;
		int slowPeriod = 4;
		int signalPeriod = 2;
		
		EMAAggregate fastAgg = new EMAAggregate(fastPeriod);
		EMAAggregate slowAgg = new EMAAggregate(slowPeriod);
		EMAAggregate signalAgg = new EMAAggregate(signalPeriod);
		
		MACDHistogramAggregate a = new MACDHistogramAggregate(fastPeriod, slowPeriod, signalPeriod);
		
		double value, macd, signal, histogram;
		MACDHistogramRecord data;
		
		// 1
		value = 1.6;
		
		fastAgg.aggregate(value);
		slowAgg.aggregate(value);
		
		assertNull(a.aggregate(value));
		
		// 2
		value = 2.5;
		fastAgg.aggregate(value);
		slowAgg.aggregate(value);
		
		assertNull(a.aggregate(value));
		
		// 3
		value = 3.4;
		fastAgg.aggregate(value);
		slowAgg.aggregate(value);
		
		assertNull(a.aggregate(value));
		
		// 4
		value = 4.3;
		macd = fastAgg.aggregate(value).getValue() - slowAgg.aggregate(value).getValue();
		signalAgg.aggregate(macd);
		
		assertNull(a.aggregate(value));
		
		// 5
		value = 5.2;
		macd = fastAgg.aggregate(value).getValue() - slowAgg.aggregate(value).getValue();
		signal = signalAgg.aggregate(macd).getValue();
		histogram = macd - signal;
		
		data = a.aggregate(value);
		
		assertNull(data.getKey());
		assertEquals(macd, data.getMacd());
		assertEquals(signal, data.getSignal());
		assertEquals(histogram, data.getHistogram());
		
		// 6
		value = 6.1;
		macd = fastAgg.aggregate(value).getValue() - slowAgg.aggregate(value).getValue();
		signal = signalAgg.aggregate(macd).getValue();
		histogram = macd - signal;
		
		data = a.aggregate(value);
		MACDHistogramRecord data_6 = data;
		
		assertNull(data.getKey());
		assertEquals(macd, data.getMacd());
		assertEquals(signal, data.getSignal());
		assertEquals(histogram, data.getHistogram());
		
		// 7
		value = 7.0;
		macd = fastAgg.aggregate(value).getValue() - slowAgg.aggregate(value).getValue();
		signal = signalAgg.aggregate(macd).getValue();
		histogram = macd - signal;
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(a);
		a = mapper.readValue(json, MACDHistogramAggregate.class);
		
		data = a.aggregate(value);
		
		assertNull(data.getKey());
		assertEquals(macd, data.getMacd());
		assertEquals(macd - data_6.getMacd(), data.getMChange());
		assertEquals(signal, data.getSignal());
		assertEquals(signal - data_6.getSignal(), data.getSChange());
		assertEquals(histogram, data.getHistogram());
		assertEquals(histogram - data_6.getHistogram(), data.getHChange());
		
	}

}
