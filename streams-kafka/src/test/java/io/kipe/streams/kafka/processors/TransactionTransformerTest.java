/*
 * Kipes SDK for Kafka - The High-Level Event Processing SDK.
 * Copyright © 2023 kipe.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
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

/**
 * Test class for {@link TransactionTransformer}.
 */
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

	/**
	 * Test that when a record with a non-starting value is passed in, it is ignored and no data is written to the
	 * store.
	 */
	@Test
	void test_transform__no_txn_ignores_not_starting_record() {
		TransactionTransformer<String, TestValue, String> t = createTransactionTransformer();
		
		when(stateStoreMock.get(KEY)).thenReturn(null);

		assertNull(t.transform(KEY, createTestValue("value")));
		
		// no data gets written to the store
		
		// verification happens at afterEach
	}

	/**
	 * Test that when a record with a starting value is passed in, it is identified as the start of a new transaction
	 * and data is written to the store.
	 */
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

	/**
	 * Test that when a record with an ongoing value is passed in and a transaction has been started, it is identified
	 * as part of the ongoing transaction and data is written to the store.
	 */
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

	/**
	 * Test that when a record with an end value is passed in and a transaction has been started, it is identified as
	 * the end of the ongoing transaction and data is written to the store.
	 */
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

	/**
	 * Tests that when the TransactionTransformer is configured to emit all records, it emits all records in the
	 * transaction.
	 */
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

	/**
	 * Tests that when the TransactionTransformer is configured to emit all records and the start and end records are
	 * the same, it emits the single record.
	 */
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

	/**
	 * Tests that when the TransactionTransformer is configured to emit only the start record, it emits only the start
	 * record.
	 */
	@Test
	void test_transform__emits_START() {
		TransactionRecord<String, TestValue> r = 
				doTransaction(
						createTransactionTransformer(EmitType.START));
		
		assertNotNull(r);
		assertEquals(1, r.getRecords().size());
		
		assertEquals(START, r.getRecord(0).value);
	}

	// TODO
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

	// TODO
	@Test
	void test_transform__emits_ONGOING() {
		TransactionRecord<String, TestValue> r = 
				doTransaction(
						createTransactionTransformer(EmitType.ONGOING));
		
		assertNotNull(r);
		assertEquals(1, r.getRecords().size());
		
		assertEquals(ONGOING, r.getRecord(0).value);
	}

	// TODO
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

	/**
	 * Test that the transform method emits an END record when EmitType is set to END.
	 * <p>
	 * This test uses the doTransaction helper method to create and perform a transaction.
	 */
	@Test
	void test_transform__emits_END() {
		TransactionRecord<String, TestValue> r = 
				doTransaction(
						createTransactionTransformer(EmitType.END));
		
		assertNotNull(r);
		assertEquals(1, r.getRecords().size());
		
		assertEquals(END, r.getRecord(0).value);
	}

	/**
	 * Test that the transform method emits an END record when EmitType is set to END and the start and end values are
	 * the same.
	 * <p>
	 * This test uses the createStartEndTransactionTransformer helper method to create a transformer with the same start
	 * and end values.
	 */
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

	/**
	 * Test that the {@link TransactionTransformer#transform} method emits a START and END record when the EmitType is
	 * set to START_AND_END.
	 */
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

	/**
	 * Test that the {@link TransactionTransformer#transform} method emits a START and END record when the EmitType is
	 * set to START_AND_END and the start and end values are the same.
	 */
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

	/**
	 * Perform a transaction using a TransactionTransformer.
	 * <p>
	 * It starts by preparing mock stateStore for the transaction, and then calls the transform method on the given
	 * TransactionTransformer.
	 * <p>
	 * It verifies that the stateStore's put method is called with the correct parameters.
	 *
	 * @param t - The TransactionTransformer to use for the transaction.
	 * @return A TransactionRecord containing the value of the transaction.
	 */
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
	 * Create a new TransactionTransformer with the following parameters:
	 * <pre>
	 * groupKeyFunction : (key, value) -> key
	 * startsWithPredicate: (key, value) -> "START".equals(value.value)
	 * endsWithPredicate : (key, value) -> "END".equals(value.value)
	 * emitType : ALL
	 * </pre>
	 *
	 * @return the created TransactionTransformer
	 */
	private TransactionTransformer<String, TestValue, String> createTransactionTransformer() {
		return createTransactionTransformer(EmitType.ALL);
	}

	/**
	 * Create a new TransactionTransformer with the following parameters:
	 * <pre>
	 * groupKeyFunction : (key, value) -> key
	 * startsWithPredicate: (key, value) -> "START".equals(value.value)
	 * endsWithPredicate : (key, value) -> "END".equals(value.value)
	 * emitType : emitType
	 * </pre>
	 *
	 * @param emitType The emitType to use for the TransactionTransformer.
	 * @return the created TransactionTransformer
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
	 * Create a new TransactionTransformer with the following parameters:
	 * <pre>
	 * groupKeyFunction   : (key, value) -> key
	 * startsWithPredicate: (key, value) -> true
	 * endsWithPredicate  : (key, value) -> true
	 * emitType           : emitType
	 * </pre>
	 *
	 * @param emitType The emitType to use for the TransactionTransformer.
	 * @return the created TransactionTransformer
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

	// TODO
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

	/**
	 * Inner class representing a test value.
	 */
	@Data
	@EqualsAndHashCode
	@ToString
	@NoArgsConstructor
	@AllArgsConstructor
	private static class TestValue {
		private String value;
	}
}
