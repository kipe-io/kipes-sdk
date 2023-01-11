package io.kipe.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kipe.streams.kafka.processors.TransactionBuilder.EmitType;
import io.kipe.streams.kafka.processors.TransactionBuilder.TransactionTransformer;
import io.kipe.streams.recordtypes.TransactionRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ExtendWith(MockitoExtension.class)
class TransactionTransformerTest {

	private static final String KEY = "key";
	
	private static final String START = "START";
	private static final String ONGOING = "ongoing";
	private static final String END = "END";
	
	@Mock
	private KeyValueStore<String, TransactionRecord<String, TestValue>> stateStoreMock;
	
	@Captor
	private ArgumentCaptor<TransactionRecord<String, TestValue>> transactionRecordCaptor;
	
	@AfterEach
	void afterEach() {
		verifyNoMoreInteractions(
				stateStoreMock);
	}
	
	// ------------------------------------------------------------------------
	// startsWith
	// ------------------------------------------------------------------------

	@Test
	void test_transform__no_txn_ignores_not_starting_record() {
		TransactionTransformer<String, TestValue, String> t = createTransactionTransformer();
		
		when(stateStoreMock.get(KEY)).thenReturn(null);

		assertNull(t.transform(KEY, createTestValue("value")));
		
		// no data gets written to the store
		
		// verification happens at afterEach
	}

	@Test
	void test_transform__no_txn_identifies_starting_record() {
		TransactionTransformer<String, TestValue, String> t = createTransactionTransformer();
		
		when(stateStoreMock.get(KEY)).thenReturn(null);
		doNothing().when(stateStoreMock).put(eq(KEY), notNull());
		
		assertNull(t.transform(KEY, createTestValue(START)));
		
		// verification happens at afterEach		
	}
	
	// ------------------------------------------------------------------------
	// ongoing
	// ------------------------------------------------------------------------

	@Test
	void test_transform__started_txn_identifies_ongoing_record() {
		TransactionTransformer<String, TestValue, String> t = createTransactionTransformer();
		
		// start transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(null);
		doNothing().when(stateStoreMock).put(eq(KEY), notNull());

		// start
		t.transform(KEY, createTestValue(START));
		
		verify(stateStoreMock).put(eq(KEY), transactionRecordCaptor.capture());

		// ongoing transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(transactionRecordCaptor.getValue());
		doNothing().when(stateStoreMock).put(eq(KEY), notNull());
		
		// ongoing
		assertNull(t.transform(KEY, createTestValue("value")));
		
		// verification happens at afterEach
	}
	
	// ------------------------------------------------------------------------
	// endsWith
	// ------------------------------------------------------------------------

	@Test
	void test_transform__started_txn_identifies_ending_record() {
		TransactionTransformer<String, TestValue, String> t = createTransactionTransformer();
		
		// start transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(null);
		doNothing().when(stateStoreMock).put(eq(KEY), notNull());

		// start
		t.transform(KEY, createTestValue(START));
		
		verify(stateStoreMock).put(eq(KEY), transactionRecordCaptor.capture());

		// end transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(transactionRecordCaptor.getValue());
		when(stateStoreMock.delete(eq(KEY))).thenReturn(null);
		
		// end
		assertNotNull(t.transform(KEY, createTestValue(END)));
		
		// verification happens at afterEach
	}
	
	// ------------------------------------------------------------------------
	// emit ALL
	// ------------------------------------------------------------------------

	@Test
	void test_transform__emits_ALL() {
		TransactionRecord<String, TestValue> r = 
				doTransaction(
						createTransactionTransformer(EmitType.ALL));
		
		assertNotNull(r);
		assertEquals(3, r.getRecords().size());
		
		assertEquals(START, r.getRecord(0).value);
		assertEquals(ONGOING, r.getRecord(1).value);
		assertEquals(END, r.getRecord(2).value);
	}
	
	@Test
	void test_transform__emits_ALL_when_START_eq_END() {
		TransactionTransformer<String, TestValue, String> t = createStartEndTransactionTransformer(EmitType.ALL);
		
		// start transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(null);
		when(stateStoreMock.delete(eq(KEY))).thenReturn(null);

		// start
		TransactionRecord<String, TestValue> r = t.transform(KEY, createTestValue("other")).value;
		
		assertNotNull(r);
		assertEquals(1, r.getRecords().size());
	}
	
	// ------------------------------------------------------------------------
	// emit START
	// ------------------------------------------------------------------------

	@Test
	void test_transform__emits_START() {
		TransactionRecord<String, TestValue> r = 
				doTransaction(
						createTransactionTransformer(EmitType.START));
		
		assertNotNull(r);
		assertEquals(1, r.getRecords().size());
		
		assertEquals(START, r.getRecord(0).value);
	}
	
	@Test
	void test_transform__emits_START_when_START_eq_END() {
		TransactionTransformer<String, TestValue, String> t = createStartEndTransactionTransformer(EmitType.START);
		
		// start transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(null);
		when(stateStoreMock.delete(eq(KEY))).thenReturn(null);

		// start
		TransactionRecord<String, TestValue> r = t.transform(KEY, createTestValue("other")).value;
		
		assertNotNull(r);
		assertEquals(1, r.getRecords().size());
	}
	
	// ------------------------------------------------------------------------
	// emit ONGOING
	// ------------------------------------------------------------------------

	@Test
	void test_transform__emits_ONGOING() {
		TransactionRecord<String, TestValue> r = 
				doTransaction(
						createTransactionTransformer(EmitType.ONGOING));
		
		assertNotNull(r);
		assertEquals(1, r.getRecords().size());
		
		assertEquals(ONGOING, r.getRecord(0).value);
	}
	
	@Test
	void test_transform__emits_ONGOING_when_START_eq_END() {
		TransactionTransformer<String, TestValue, String> t = createStartEndTransactionTransformer(EmitType.ONGOING);
		
		// start transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(null);
		when(stateStoreMock.delete(eq(KEY))).thenReturn(null);

		// start
		TransactionRecord<String, TestValue> r = t.transform(KEY, createTestValue("other")).value;
		
		assertNotNull(r);
		assertTrue(r.getRecords().isEmpty());
	}
	
	// ------------------------------------------------------------------------
	// emit END
	// ------------------------------------------------------------------------

	@Test
	void test_transform__emits_END() {
		TransactionRecord<String, TestValue> r = 
				doTransaction(
						createTransactionTransformer(EmitType.END));
		
		assertNotNull(r);
		assertEquals(1, r.getRecords().size());
		
		assertEquals(END, r.getRecord(0).value);
	}
	
	@Test
	void test_transform__emits_END_when_START_eq_END() {
		TransactionTransformer<String, TestValue, String> t = createStartEndTransactionTransformer(EmitType.END);
		
		// start transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(null);
		when(stateStoreMock.delete(eq(KEY))).thenReturn(null);

		// start
		TransactionRecord<String, TestValue> r = t.transform(KEY, createTestValue("other")).value;
		
		assertNotNull(r);
		assertEquals(1, r.getRecords().size());
	}
	
	// ------------------------------------------------------------------------
	// emit START_AND_END
	// ------------------------------------------------------------------------

	@Test
	void test_transform__emits_START_AND_END() {
		TransactionRecord<String, TestValue> r = 
				doTransaction(
						createTransactionTransformer(EmitType.START_AND_END));
		
		assertNotNull(r);
		assertEquals(2, r.getRecords().size());
		
		assertEquals(START, r.getRecord(0).value);
		assertEquals(END, r.getRecord(1).value);
	}
	
	@Test
	void test_transform__emits_START_AND_END_when_START_eq_END() {
		TransactionTransformer<String, TestValue, String> t = createStartEndTransactionTransformer(EmitType.START_AND_END);
		
		// start transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(null);
		when(stateStoreMock.delete(eq(KEY))).thenReturn(null);

		// start
		TransactionRecord<String, TestValue> r = t.transform(KEY, createTestValue("other")).value;
		
		assertNotNull(r);
		assertEquals(1, r.getRecords().size());
	}
	
	// ------------------------------------------------------------------------
	// utils 
	// ------------------------------------------------------------------------
	
	private TransactionRecord<String, TestValue> doTransaction(TransactionTransformer<String, TestValue, String> t) {
		// start transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(null);
		doNothing().when(stateStoreMock).put(eq(KEY), notNull());

		// start
		t.transform(KEY, createTestValue(START));
		
		verify(stateStoreMock).put(eq(KEY), transactionRecordCaptor.capture());

		// ongoing transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(transactionRecordCaptor.getValue());
		doNothing().when(stateStoreMock).put(eq(KEY), notNull());
		
		// ongoing
		t.transform(KEY, createTestValue(ONGOING));
		
		verify(stateStoreMock, times(2)).put(eq(KEY), transactionRecordCaptor.capture());

		// end transaction - prep mock
		when(stateStoreMock.get(KEY)).thenReturn(transactionRecordCaptor.getValue());
		when(stateStoreMock.delete(eq(KEY))).thenReturn(null);
		
		// end
		return t.transform(KEY, createTestValue(END)).value;
	}

	/**
	 * <pre>
	 * groupKeyFunction   : (key, value) -> key
	 * startsWithPredicate: (key, value) -> "START".equals(value.value)
	 * endsWithPredicate  : (key, value) -> "END".equals(value.value)
	 * emitType           : ALL
	 * </pre> 
	 */
	private TransactionTransformer<String, TestValue, String> createTransactionTransformer() {
		return createTransactionTransformer(EmitType.ALL);
	}
		
	/**
	 * <pre>
	 * groupKeyFunction   : (key, value) -> key
	 * startsWithPredicate: (key, value) -> "START".equals(value.value)
	 * endsWithPredicate  : (key, value) -> "END".equals(value.value)
	 * emitType           : emitType
	 * </pre> 
	 */
	private TransactionTransformer<String, TestValue, String> createTransactionTransformer(EmitType emitType) {
		TransactionTransformer<String, TestValue, String> t = new TransactionTransformer<>(
				null, 
				(key, value) -> key, 
				(key, value) -> START.equals(value.value), 
				(key, value) -> END.equals(value.value), 
				emitType);
		
		t.stateStore = stateStoreMock;
		
		return t;
	}
	
	/**
	 * <pre>
	 * groupKeyFunction   : (key, value) -> key
	 * startsWithPredicate: (key, value) -> true
	 * endsWithPredicate  : (key, value) -> true
	 * emitType           : emitType
	 * </pre> 
	 */
	private TransactionTransformer<String, TestValue, String> createStartEndTransactionTransformer(EmitType emitType) {
		TransactionTransformer<String, TestValue, String> t = new TransactionTransformer<>(
				null, 
				(key, value) -> key, 
				(key, value) -> true, 
				(key, value) -> true, 
				emitType);
		
		t.stateStore = stateStoreMock;
		
		return t;
	}
	
	/**
	 * <pre>
	 * key  : { symbol: SYMBOL, timestamp: System.currentTimeMillis() }
	 * value: value
	 * </pre> 
	 */
	private TestValue createTestValue(String value) {
		return new TestValue(value);
	}
	
	// ------------------------------------------------------------------------
	// inner class - TestValue
	// ------------------------------------------------------------------------

	@Data
	@EqualsAndHashCode
	@ToString
	@NoArgsConstructor
	@AllArgsConstructor
	private static class TestValue {
		private String value;
	}
}
