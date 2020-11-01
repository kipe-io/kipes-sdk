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

import de.tradingpulse.common.stream.aggregates.EMAAggregate;
import de.tradingpulse.common.stream.aggregates.IncrementalAggregate;
import de.tradingpulse.common.stream.recordtypes.DoubleData;
import de.tradingpulse.common.stream.recordtypes.OHLCVRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;

@ExtendWith(MockitoExtension.class)
class IncrementalEMATransformerTest {

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
	void test_usecase() {
		IncrementalEMATransformer t = createIncrementalEMATransformer(13);
		IncrementalAggregate<EMAAggregate> incAgg = new IncrementalAggregate<>();
		
		when(stateMock.get(SYMBOL)).thenReturn(incAgg);
		doNothing().when(stateMock).put(SYMBOL, incAgg);
		
		EMAAggregate emaAgg = new EMAAggregate(13);
		
		// week 1
		addRecord(t, 1567382400000L, 205.7);
		addRecord(t, 1567382400000L, 209.19);
		addRecord(t, 1567382400000L, 213.28);
		DoubleData emaT = addRecord(t, 1567382400000L, 213.26);
		DoubleData ema = emaAgg.aggregate(213.26);
		
		assertNull(emaT);
		assertNull(ema);
		
		// week 2
		addRecord(t, 1567987200000L, 214.17);
		addRecord(t, 1567987200000L, 216.7);
		addRecord(t, 1567987200000L, 223.59);
		addRecord(t, 1567987200000L, 223.09);
		emaT = addRecord(t, 1567987200000L, 218.75);
		ema = emaAgg.aggregate(218.75);
		
		assertNull(emaT);
		assertNull(ema);
				
		// week 3
		addRecord(t, 1568592000000L, 219.9);
		addRecord(t, 1568592000000L, 220.7);
		addRecord(t, 1568592000000L, 222.77);
		addRecord(t, 1568592000000L, 220.96);
		emaT = addRecord(t, 1568592000000L, 217.73);
		ema = emaAgg.aggregate(217.73);

		assertNull(emaT);
		assertNull(ema);
		
		// week 4
		addRecord(t, 1569196800000L, 218.72);
		addRecord(t, 1569196800000L, 217.68);
		addRecord(t, 1569196800000L, 221.03);
		addRecord(t, 1569196800000L, 219.89);
		emaT = addRecord(t, 1569196800000L, 218.82);
		ema = emaAgg.aggregate(218.82);

		assertNull(emaT);
		assertNull(ema);
		
		// week 5
		addRecord(t, 1569801600000L, 223.97);
		addRecord(t, 1569801600000L, 224.59);
		addRecord(t, 1569801600000L, 218.96);
		addRecord(t, 1569801600000L, 220.82);
		emaT = addRecord(t, 1569801600000L, 227.01);
		ema = emaAgg.aggregate(227.01);

		assertNull(emaT);
		assertNull(ema);
		
		// week 6
		addRecord(t, 1570406400000L, 227.06);
		addRecord(t, 1570406400000L, 224.4);
		addRecord(t, 1570406400000L, 227.03);
		addRecord(t, 1570406400000L, 230.09);
		emaT = addRecord(t, 1570406400000L, 236.21);
		ema = emaAgg.aggregate(236.21);

		assertNull(emaT);
		assertNull(ema);
		
		// week 7
		addRecord(t, 1571011200000L, 235.87);
		addRecord(t, 1571011200000L, 235.32);
		addRecord(t, 1571011200000L, 234.37);
		addRecord(t, 1571011200000L, 235.28);
		emaT = addRecord(t, 1571011200000L, 236.41);
		ema = emaAgg.aggregate(236.41);

		assertNull(emaT);
		assertNull(ema);
		
		// week 8
		addRecord(t, 1571616000000L, 240.51);
		addRecord(t, 1571616000000L, 239.96);
		addRecord(t, 1571616000000L, 243.18);
		addRecord(t, 1571616000000L, 243.58);
		emaT = addRecord(t, 1571616000000L, 246.58);
		ema = emaAgg.aggregate(246.58);

		assertNull(emaT);
		assertNull(ema);
		
		// week 9
		addRecord(t, 1572220800000L, 249.05);
		addRecord(t, 1572220800000L, 243.29);
		addRecord(t, 1572220800000L, 243.26);
		addRecord(t, 1572220800000L, 248.76);
		emaT = addRecord(t, 1572220800000L, 255.82);
		ema = emaAgg.aggregate(255.82);

		assertNull(emaT);
		assertNull(ema);
		
		// week 10
		addRecord(t, 1572825600000L, 257.5);
		addRecord(t, 1572825600000L, 257.13);
		addRecord(t, 1572825600000L, 257.24);
		addRecord(t, 1572825600000L, 259.43);
		emaT = addRecord(t, 1572825600000L, 260.14);
		ema = emaAgg.aggregate(260.14);

		assertNull(emaT);
		assertNull(ema);
		
		// week 11
		addRecord(t, 1573430400000L, 262.2);
		addRecord(t, 1573430400000L, 261.96);
		addRecord(t, 1573430400000L, 264.47);
		addRecord(t, 1573430400000L, 262.64);
		emaT = addRecord(t, 1573430400000L, 265.76);
		ema = emaAgg.aggregate(265.76);

		assertNull(emaT);
		assertNull(ema);
		
		// week 12
		addRecord(t, 1574035200000L, 267.1);
		addRecord(t, 1574035200000L, 266.29);
		addRecord(t, 1574035200000L, 263.19);
		addRecord(t, 1574035200000L, 262.01);
		emaT = addRecord(t, 1574035200000L, 261.78);
		ema = emaAgg.aggregate(261.78);

		assertNull(emaT);
		assertNull(ema);
		
		// week 13
		addRecord(t, 1574640000000L, 266.37);
		addRecord(t, 1574640000000L, 264.29);
		addRecord(t, 1574640000000L, 267.84);
		emaT = addRecord(t, 1574640000000L, 267.25);
		ema = emaAgg.aggregate(267.25);

		assertEquals(ema.getValue(), emaT.getValue());
		
		// week 14
		addRecord(t, 1575244800000L, 264.16);
		addRecord(t, 1575244800000L, 259.45);
		addRecord(t, 1575244800000L, 261.74);
		addRecord(t, 1575244800000L, 265.58);
		emaT = addRecord(t, 1575244800000L, 270.71);
		ema = emaAgg.aggregate(270.71);

		assertEquals(ema.getValue(), emaT.getValue());
				
		// week 15
		addRecord(t, 1575849600000L, 266.92);
		addRecord(t, 1575849600000L, 268.48);
		addRecord(t, 1575849600000L, 270.77);
		addRecord(t, 1575849600000L, 271.46);
		emaT = addRecord(t, 1575849600000L, 275.15);
		ema = emaAgg.aggregate(275.15);

		assertEquals(ema.getValue(), emaT.getValue());
		
	}
	
	// -------------------------------------------------------------------------
	// test init
	// -------------------------------------------------------------------------

	@Test
	void test_init__store_was_aquired_with_correct_name() {
		IncrementalEMATransformer t = new IncrementalEMATransformer(STORE_NAME, 1);
		
		when(processorCtxMock.getStateStore(STORE_NAME)).thenReturn(stateMock);
		
		t.init(processorCtxMock);
	}
	
	// -------------------------------------------------------------------------
	// test transform
	// -------------------------------------------------------------------------

	@Test
	void test_transform__ema_2_first_value_is_null() {
		IncrementalEMATransformer t = createIncrementalEMATransformer(2);

		when(stateMock.get(SYMBOL)).thenReturn(null);

		SymbolTimestampKey key = createKey(0);
		OHLCVRecord value = createValue(key, 1.0);
		
		KeyValue<SymbolTimestampKey, DoubleData> keyValue = t.transform(key, value);
		
		assertNull(keyValue);
		verify(stateMock).put(eq(SYMBOL), notNull());
	}
	
	@Test
	void test_transform__ema_2_without_increments() {
		IncrementalEMATransformer t = createIncrementalEMATransformer(2);
		IncrementalAggregate<EMAAggregate> incAgg = new IncrementalAggregate<>();
				
		when(stateMock.get(SYMBOL)).thenReturn(incAgg);
		doNothing().when(stateMock).put(SYMBOL, incAgg);
		
		// comparision EMAAggregate
		EMAAggregate emaAgg = new EMAAggregate(2);
		
		// record 1 - ts 0 -----------------------------------------------------
		long timestamp = 0;
		double close = 1.0;
		DoubleData ema = emaAgg.aggregate(close);
		
		DoubleData emaT = addRecord(t, timestamp, close);
		
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
		IncrementalEMATransformer t = createIncrementalEMATransformer(2);
		IncrementalAggregate<EMAAggregate> incAgg = new IncrementalAggregate<>();
				
		when(stateMock.get(SYMBOL)).thenReturn(incAgg);
		doNothing().when(stateMock).put(SYMBOL, incAgg);
		
		// comparision EMAAggregate
		EMAAggregate emaAgg = new EMAAggregate(2);
		
		// record 1 - ts 0 -----------------------------------------------------
		long timestamp = 0;
		double close = 1.0;
		DoubleData ema = emaAgg.aggregate(close);
		
		DoubleData emaT = addRecord(t, timestamp, close);
		
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
		IncrementalEMATransformer t = createIncrementalEMATransformer(2);
		IncrementalAggregate<EMAAggregate> incAgg = new IncrementalAggregate<>();
				
		when(stateMock.get(SYMBOL)).thenReturn(incAgg);
		doNothing().when(stateMock).put(SYMBOL, incAgg);
		
		// comparision EMAAggregate
		EMAAggregate emaAgg = new EMAAggregate(2);
		
		// record 1 - ts 0 -----------------------------------------------------
		long timestamp = 0;
		double close = 1.0;
		DoubleData ema = emaAgg.aggregate(close);
		
		DoubleData emaT = addRecord(t, timestamp, close);
		
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
		IncrementalEMATransformer t = createIncrementalEMATransformer(2);
		IncrementalAggregate<EMAAggregate> incAgg = new IncrementalAggregate<>();
				
		when(stateMock.get(SYMBOL)).thenReturn(incAgg);
		doNothing().when(stateMock).put(SYMBOL, incAgg);
		
		// comparision EMAAggregate
		EMAAggregate emaAgg = new EMAAggregate(2);
		
		// record 1 - ts 0 -----------------------------------------------------
		long timestamp = 0;
		double close = 1.0;
		DoubleData ema = emaAgg.aggregate(close);
		
		DoubleData emaT = addRecord(t, timestamp, close);
		
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
	
	private DoubleData addRecord(IncrementalEMATransformer t, long timestamp, double close) {
		SymbolTimestampKey key = createKey(timestamp);
		OHLCVRecord value = createValue(key, close);
		
		KeyValue<SymbolTimestampKey, DoubleData> keyValue = t.transform(key, value);
		
		return keyValue == null? null : keyValue.value;
	}

	private IncrementalEMATransformer createIncrementalEMATransformer(int numObservations) {
		IncrementalEMATransformer t = new IncrementalEMATransformer(STORE_NAME, numObservations);
		
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
	
	private OHLCVRecord createValue(SymbolTimestampKey key, Double close) {
		return OHLCVRecord.builder()
				.key(key)
				.open(null)
				.high(null)
				.low(null)
				.close(close)
				.build();
	}
}
