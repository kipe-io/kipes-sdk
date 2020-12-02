package de.tradingpulse.stage.indicators.service.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import de.tradingpulse.common.stream.aggregates.IncrementalAggregate;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TimeRange;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stages.indicators.aggregates.EMAAggregate;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;

@ExtendWith(MockitoExtension.class)
class EMATransformerTest {

	private static final String STORE_NAME = "storeName";
	private static final String SYMBOL = "symbol";

	@Mock
	private ProcessorContext processorCtxMock;
	
	@Mock
	private KeyValueStore<String, IncrementalAggregate<EMAAggregate>> stateMock;
	
	@AfterEach
	void afterEach() {
		verifyNoMoreInteractions(
				processorCtxMock,
				stateMock);
	}

	// -------------------------------------------------------------------------
	// test transform
	// -------------------------------------------------------------------------
	
	@Test
	void test_usecase_weekly() {
		// test for correct incremental calculation
		
		EMATransformer t = createIncrementalEMATransformer(13);
		IncrementalAggregate<EMAAggregate> incAgg = new IncrementalAggregate<>();
		
		when(stateMock.get(SYMBOL)).thenReturn(incAgg);
		doNothing().when(stateMock).put(SYMBOL, incAgg);
		
		EMAAggregate emaAgg = new EMAAggregate(13);
		
		// week 1
		addWeeklyRecord(t, 1567382400000L, 205.7);
		addWeeklyRecord(t, 1567382400001L, 209.19);
		addWeeklyRecord(t, 1567382400002L, 213.28);
		DoubleRecord emaT = addWeeklyRecord(t, 1567382400003L, 213.26);
		DoubleRecord ema = emaAgg.aggregate(213.26);
		
		assertNull(emaT);
		assertNull(ema);
		
		// week 2
		addWeeklyRecord(t, 1567987200000L, 214.17);
		addWeeklyRecord(t, 1567987200001L, 216.7);
		addWeeklyRecord(t, 1567987200002L, 223.59);
		addWeeklyRecord(t, 1567987200003L, 223.09);
		emaT = addWeeklyRecord(t, 1567987200004L, 218.75);
		ema = emaAgg.aggregate(218.75);
		
		assertNull(emaT);
		assertNull(ema);
				
		// week 3
		addWeeklyRecord(t, 1568592000000L, 219.9);
		addWeeklyRecord(t, 1568592000001L, 220.7);
		addWeeklyRecord(t, 1568592000002L, 222.77);
		addWeeklyRecord(t, 1568592000003L, 220.96);
		emaT = addWeeklyRecord(t, 1568592000004L, 217.73);
		ema = emaAgg.aggregate(217.73);

		assertNull(emaT);
		assertNull(ema);
		
		// week 4
		addWeeklyRecord(t, 1569196800000L, 218.72);
		addWeeklyRecord(t, 1569196800001L, 217.68);
		addWeeklyRecord(t, 1569196800002L, 221.03);
		addWeeklyRecord(t, 1569196800003L, 219.89);
		emaT = addWeeklyRecord(t, 1569196800004L, 218.82);
		ema = emaAgg.aggregate(218.82);

		assertNull(emaT);
		assertNull(ema);
		
		// week 5
		addWeeklyRecord(t, 1569801600000L, 223.97);
		addWeeklyRecord(t, 1569801600001L, 224.59);
		addWeeklyRecord(t, 1569801600002L, 218.96);
		addWeeklyRecord(t, 1569801600003L, 220.82);
		emaT = addWeeklyRecord(t, 1569801600004L, 227.01);
		ema = emaAgg.aggregate(227.01);

		assertNull(emaT);
		assertNull(ema);
		
		// week 6
		addWeeklyRecord(t, 1570406400000L, 227.06);
		addWeeklyRecord(t, 1570406400001L, 224.4);
		addWeeklyRecord(t, 1570406400002L, 227.03);
		addWeeklyRecord(t, 1570406400003L, 230.09);
		emaT = addWeeklyRecord(t, 1570406400004L, 236.21);
		ema = emaAgg.aggregate(236.21);

		assertNull(emaT);
		assertNull(ema);
		
		// week 7
		addWeeklyRecord(t, 1571011200000L, 235.87);
		addWeeklyRecord(t, 1571011200001L, 235.32);
		addWeeklyRecord(t, 1571011200002L, 234.37);
		addWeeklyRecord(t, 1571011200003L, 235.28);
		emaT = addWeeklyRecord(t, 1571011200004L, 236.41);
		ema = emaAgg.aggregate(236.41);

		assertNull(emaT);
		assertNull(ema);
		
		// week 8
		addWeeklyRecord(t, 1571616000000L, 240.51);
		addWeeklyRecord(t, 1571616000001L, 239.96);
		addWeeklyRecord(t, 1571616000002L, 243.18);
		addWeeklyRecord(t, 1571616000003L, 243.58);
		emaT = addWeeklyRecord(t, 1571616000004L, 246.58);
		ema = emaAgg.aggregate(246.58);

		assertNull(emaT);
		assertNull(ema);
		
		// week 9
		addWeeklyRecord(t, 1572220800000L, 249.05);
		addWeeklyRecord(t, 1572220800001L, 243.29);
		addWeeklyRecord(t, 1572220800002L, 243.26);
		addWeeklyRecord(t, 1572220800003L, 248.76);
		emaT = addWeeklyRecord(t, 1572220800004L, 255.82);
		ema = emaAgg.aggregate(255.82);

		assertNull(emaT);
		assertNull(ema);
		
		// week 10
		addWeeklyRecord(t, 1572825600000L, 257.5);
		addWeeklyRecord(t, 1572825600001L, 257.13);
		addWeeklyRecord(t, 1572825600002L, 257.24);
		addWeeklyRecord(t, 1572825600003L, 259.43);
		emaT = addWeeklyRecord(t, 1572825600004L, 260.14);
		ema = emaAgg.aggregate(260.14);

		assertNull(emaT);
		assertNull(ema);
		
		// week 11
		addWeeklyRecord(t, 1573430400000L, 262.2);
		addWeeklyRecord(t, 1573430400001L, 261.96);
		addWeeklyRecord(t, 1573430400002L, 264.47);
		addWeeklyRecord(t, 1573430400003L, 262.64);
		emaT = addWeeklyRecord(t, 1573430400004L, 265.76);
		ema = emaAgg.aggregate(265.76);

		assertNull(emaT);
		assertNull(ema);
		
		// week 12
		addWeeklyRecord(t, 1574035200000L, 267.1);
		addWeeklyRecord(t, 1574035200001L, 266.29);
		addWeeklyRecord(t, 1574035200002L, 263.19);
		addWeeklyRecord(t, 1574035200003L, 262.01);
		emaT = addWeeklyRecord(t, 1574035200004L, 261.78);
		ema = emaAgg.aggregate(261.78);

		assertNull(emaT);
		assertNull(ema);
		
		// week 13
		addWeeklyRecord(t, 1574640000000L, 266.37);
		addWeeklyRecord(t, 1574640000001L, 264.29);
		addWeeklyRecord(t, 1574640000002L, 267.84);
		emaT = addWeeklyRecord(t, 1574640000003L, 267.25);
		ema = emaAgg.aggregate(267.25);

		assertEquals(ema.getValue(), emaT.getValue());
		
		// week 14
		addWeeklyRecord(t, 1575244800000L, 264.16);
		addWeeklyRecord(t, 1575244800001L, 259.45);
		addWeeklyRecord(t, 1575244800002L, 261.74);
		addWeeklyRecord(t, 1575244800003L, 265.58);
		emaT = addWeeklyRecord(t, 1575244800004L, 270.71);
		ema = emaAgg.aggregate(270.71);

		assertEquals(ema.getValue(), emaT.getValue());
				
		// week 15
		addWeeklyRecord(t, 1575849600000L, 266.92);
		addWeeklyRecord(t, 1575849600001L, 268.48);
		addWeeklyRecord(t, 1575849600002L, 270.77);
		addWeeklyRecord(t, 1575849600003L, 271.46);
		emaT = addWeeklyRecord(t, 1575849600004L, 275.15);
		ema = emaAgg.aggregate(275.15);

		assertEquals(ema.getValue(), emaT.getValue());
		
	}
	
	// -------------------------------------------------------------------------
	// test init
	// -------------------------------------------------------------------------

	@Test
	void test_init__store_was_aquired_with_correct_name() {
		// validation happens at #afterEach()
		
		EMATransformer t = new EMATransformer(STORE_NAME, 1);
		
		when(processorCtxMock.getStateStore(STORE_NAME)).thenReturn(stateMock);
		
		t.init(processorCtxMock);
	}
	
	// -------------------------------------------------------------------------
	// test transform
	// -------------------------------------------------------------------------

	@Test
	void test_transform__ema_2_first_value_is_null() {
		EMATransformer t = createIncrementalEMATransformer(2);

		when(stateMock.get(SYMBOL)).thenReturn(null);

		SymbolTimestampKey key = createKey(0);
		OHLCVRecord value = createValue(key, TimeRange.MILLISECOND, 1.0);
		
		KeyValue<SymbolTimestampKey, DoubleRecord> keyValue = t.transform(key, value);
		
		assertNull(keyValue);
		verify(stateMock).put(eq(SYMBOL), notNull());
	}
	
	@Test
	void test_transform__ema_2_without_increments() {
		EMATransformer t = createIncrementalEMATransformer(2);
		IncrementalAggregate<EMAAggregate> incAgg = new IncrementalAggregate<>();
				
		when(stateMock.get(SYMBOL)).thenReturn(incAgg);
		doNothing().when(stateMock).put(SYMBOL, incAgg);
		
		// comparision EMAAggregate
		EMAAggregate emaAgg = new EMAAggregate(2);
		
		// record 1 - ts 0 -----------------------------------------------------
		long timestamp = 0;
		double close = 1.0;
		DoubleRecord ema = emaAgg.aggregate(close);
		
		DoubleRecord emaT = addRecord(t, timestamp, close);
		
		assertNull(emaT);
		
		// record 2 - ts 1 -----------------------------------------------------
		timestamp = 1;
		close = 2.0;
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
		
		// record 3 - ts 2 -----------------------------------------------------
		timestamp = 2;
		close = 3.0;
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
		
		// record 4 - ts 3 -----------------------------------------------------
		timestamp = 3;
		close = 4.0;
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
		
	}
	
	@Test
	void test_transform__ema_2_with_increments_on_first_timestamp() {
		EMATransformer t = createIncrementalEMATransformer(2);
		IncrementalAggregate<EMAAggregate> incAgg = new IncrementalAggregate<>();
				
		when(stateMock.get(SYMBOL)).thenReturn(incAgg);
		doNothing().when(stateMock).put(SYMBOL, incAgg);
		
		// comparision EMAAggregate
		EMAAggregate emaAgg = new EMAAggregate(2);
		
		// record 1 - ts 0 -----------------------------------------------------
		long timestamp = 0;
		double close = 1.0;
		DoubleRecord ema = emaAgg.aggregate(close);
		
		DoubleRecord emaT = addRecord(t, timestamp, close);
		
		assertNull(emaT);
		
		// record 2 - ts 0 -----------------------------------------------------
		// same timestamp as before
		close = 2.0;
		emaAgg = new EMAAggregate(2);  // increment overwrite
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertNull(emaT);
		
		// record 3 - ts 1 -----------------------------------------------------
		timestamp = 1;
		close = 3.0;
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
		
		// record 4 - ts 2 -----------------------------------------------------
		timestamp = 4;
		close = 4.0;
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
	}
	
	@Test
	void test_transform__ema_2_with_increments_on_second_timestamp() {
		EMATransformer t = createIncrementalEMATransformer(2);
		IncrementalAggregate<EMAAggregate> incAgg = new IncrementalAggregate<>();
				
		when(stateMock.get(SYMBOL)).thenReturn(incAgg);
		doNothing().when(stateMock).put(SYMBOL, incAgg);
		
		// comparision EMAAggregate
		EMAAggregate emaAgg = new EMAAggregate(2);
		
		// record 1 - ts 0 -----------------------------------------------------
		long timestamp = 0;
		double close = 1.0;
		DoubleRecord ema = emaAgg.aggregate(close);
		
		DoubleRecord emaT = addRecord(t, timestamp, close);
		
		assertNull(emaT);
		
		// record 2 - ts 1 -----------------------------------------------------
		timestamp = 1;
		close = 2.0;
		EMAAggregate emaAggStable = emaAgg.deepClone(); // remember the state
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
		
		// record 3 - ts 1 -----------------------------------------------------
		// same timestamp as before
		close = 3.0;
		emaAgg = emaAggStable; // reset state
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
		
		// record 4 - ts 2 -----------------------------------------------------
		timestamp = 2;
		close = 4.0;
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
	}
	
	@Test
	void test_transform__ema_2_with_multiple_increments_on_third_timestamp() {
		EMATransformer t = createIncrementalEMATransformer(2);
		IncrementalAggregate<EMAAggregate> incAgg = new IncrementalAggregate<>();
				
		when(stateMock.get(SYMBOL)).thenReturn(incAgg);
		doNothing().when(stateMock).put(SYMBOL, incAgg);
		
		// comparision EMAAggregate
		EMAAggregate emaAgg = new EMAAggregate(2);
		
		// record 1 - ts 0 -----------------------------------------------------
		long timestamp = 0;
		double close = 1.0;
		DoubleRecord ema = emaAgg.aggregate(close);
		
		DoubleRecord emaT = addRecord(t, timestamp, close);
		
		assertNull(emaT);
		
		// record 2 - ts 1 -----------------------------------------------------
		timestamp = 1;
		close = 2.0;
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
		
		// record 3 - ts 2 -----------------------------------------------------
		timestamp = 2;
		close = 3.0;
		EMAAggregate emaAggStable = emaAgg.deepClone(); // remember the state
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
		
		// record 4 - ts 2 -----------------------------------------------------
		// same timestamp as before
		close = 4.0;
		emaAgg = emaAggStable.deepClone(); // reset state
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
		
		// record 5 - ts 2 -----------------------------------------------------
		// same timestamp as before
		close = 5.0;
		emaAgg = emaAggStable.deepClone(); // reset state
		ema = emaAgg.aggregate(close);
		
		emaT = addRecord(t, timestamp, close);
		
		assertEquals(ema.getValue(), emaT.getValue());
	}
	
	// -------------------------------------------------------------------------
	// utilities
	// -------------------------------------------------------------------------
	
	private DoubleRecord addWeeklyRecord(EMATransformer t, long timestamp, double close) {
		
		return addRecord(t, timestamp, TimeRange.WEEK, close);
	}
	
	private DoubleRecord addRecord(EMATransformer t, long timestamp, double close) {
		
		return addRecord(t, timestamp, TimeRange.MILLISECOND, close);
	}
	
	private DoubleRecord addRecord(EMATransformer t, long timestamp, TimeRange timeRange, double close) {
		SymbolTimestampKey key = createKey(timestamp);
		OHLCVRecord value = createValue(key, timeRange, close);
		
		KeyValue<SymbolTimestampKey, DoubleRecord> keyValue = t.transform(key, value);
		
		return keyValue == null? null : keyValue.value;
	}

	private EMATransformer createIncrementalEMATransformer(int numObservations) {
		EMATransformer t = new EMATransformer(STORE_NAME, numObservations);
		
		when(processorCtxMock.getStateStore(STORE_NAME)).thenReturn(stateMock);
		
		t.init(processorCtxMock);
		
		return t;
	}
	
	private SymbolTimestampKey createKey(long timestamp) {
		return SymbolTimestampKey.builder()
				.symbol(SYMBOL)
				.timestamp(timestamp)
				.build();
	}
	
	private OHLCVRecord createValue(SymbolTimestampKey key, TimeRange timeRange, Double close) {
		return OHLCVRecord.builder()
				.key(key)
				.timeRange(timeRange)
				.open(null)
				.high(null)
				.low(null)
				.close(close)
				.build();
	}
}
