package de.tradingpulse.streams.recordtypes;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

class TransactionRecordTest {

	private static final String GROUP_KEY = "groupKey";
	private static final String SYMBOL = "symbol";
	private static final String VALUE = "value";

	// ------------------------------------------------------------------------
	// tests createFrom
	// ------------------------------------------------------------------------

	@Test
	void test_createFrom__correctly_initializes() {
		TestValue value = createTestValue(VALUE);
		
		TransactionRecord<TestValue, Void> record = TransactionRecord.createFrom(value);
		
		assertNotNull(value.getKey());
		assertEquals(value.getKey(), record.getKey());
		assertTrue(record.getRecords().contains(value));
	}
	
	// ------------------------------------------------------------------------
	// tests addUnique
	// ------------------------------------------------------------------------

	@Test
	void test_addUnique__fails_on_this_key_null() {
		TransactionRecord<TestValue, Void> record = new TransactionRecord<>();
		TestValue value = createTestValue(VALUE);
		
		assertThrows(NullPointerException.class, () -> {
			record.addUnique(value);
		});
	}

	@Test
	void test_addUnique__fails_on_value_null() {
		TransactionRecord<TestValue, Void> record = createTransactionRecord();
		
		assertThrows(NullPointerException.class, () -> {
			record.addUnique(null);
		});
	}

	@Test
	void test_addUnique__fails_on_value_key_null() {
		TransactionRecord<TestValue, Void> record = createTransactionRecord();
		TestValue value = new TestValue();
		
		assertThrows(NullPointerException.class, () -> {
			record.addUnique(value);
		});
	}

	@Test
	void test_addUnique__adds_value() {
		TransactionRecord<TestValue, Void> record = createTransactionRecord();
		TestValue value = createTestValue(VALUE);

		record.addUnique(value);
		
		assertTrue(record.getRecords().contains(value));
	}

	@Test
	void test_addUnique__ignores_adding_already_stored_values() {
		TransactionRecord<TestValue, Void> record = createTransactionRecord();
		TestValue value = createTestValue(VALUE);

		record.addUnique(value);
		record.addUnique(value);
		
		assertEquals(1, record.getRecords().size());
	}
	
	@Test
	void test_addUnique__updates_this_timestamp() {
		TransactionRecord<TestValue, Void> record = createTransactionRecord();
		long thisTimestamp = record.getKey().getTimestamp();
		
		await().atMost(Duration.ofSeconds(1)).until(() -> System.currentTimeMillis() > thisTimestamp);
		TestValue value = createTestValue(VALUE);
		
		record.addUnique(value);
		
		assertTrue(thisTimestamp < record.getKey().getTimestamp());
		assertEquals(record.getKey().getTimestamp(), value.getKey().getTimestamp());
	}
	
	// ------------------------------------------------------------------------
	// tests getRecord
	// ------------------------------------------------------------------------

	@Test
	void test_getRecord__throws_IndexOutOfBoundsException() {
		TransactionRecord<TestValue, Void> record = createTransactionRecord();
		
		TestValue v1 = createTestValue("1");
		TestValue v2 = createTestValue("2");
		TestValue v3 = createTestValue("3");
		
		record.addUnique(v1);
		record.addUnique(v2);
		record.addUnique(v3);
		
		assertEquals(3, record.getRecords().size());
		
		assertThrows(IndexOutOfBoundsException.class, () -> record.getRecord(3));
		assertThrows(IndexOutOfBoundsException.class, () -> record.getRecord(-4));
	}

	@Test
	void test_getRecord__returns_correct_record() {
		TransactionRecord<TestValue, Void> record = createTransactionRecord();
		
		TestValue v1 = createTestValue("1");
		TestValue v2 = createTestValue("2");
		TestValue v3 = createTestValue("3");
		
		record.addUnique(v1);
		record.addUnique(v2);
		record.addUnique(v3);
		
		assertEquals(3, record.getRecords().size());

		assertEquals(v1, record.getRecord(0));
		assertEquals(v1, record.getRecord(-3));

		assertEquals(v2, record.getRecord(1));
		assertEquals(v2, record.getRecord(-2));

		assertEquals(v3, record.getRecord(2));
		assertEquals(v3, record.getRecord(-1));
	}
	
	// ------------------------------------------------------------------------
	// serde
	// ------------------------------------------------------------------------

	@Test
	void test_serde() throws JsonProcessingException {
		TransactionRecord<TestValue, String> record = createTransactionRecord();
		record.setGroupKey(GROUP_KEY);
		TestValue value = createTestValue(VALUE);

		record.addUnique(value);
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(record);
		
		TransactionRecord<?,?> r = mapper.readValue(json, TransactionRecord.class); 
		assertEquals(GROUP_KEY, r.getGroupKey());
		assertTrue(r.getRecords().contains(value));
		
	}
	
	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private static <V extends AbstractIncrementalAggregateRecord, GK> TransactionRecord<V, GK> createTransactionRecord() {
		return (TransactionRecord<V, GK>) TransactionRecord.builder()
				.key(SymbolTimestampKey.builder()
						.symbol(SYMBOL)
						.timestamp(System.currentTimeMillis())
						.build())
				.build();
	}
	
	private static TestValue createTestValue(String value) {
		return TestValue.builder()
				.key(SymbolTimestampKey.builder()
						.symbol(SYMBOL)
						.timestamp(System.currentTimeMillis())
						.build())
				.value(value)
				.build();
	}
	
	// ------------------------------------------------------------------------
	// inner class - TestValue
	// ------------------------------------------------------------------------

	@Data
	@EqualsAndHashCode(callSuper = true)
	@ToString(callSuper = true)
	@NoArgsConstructor
	@AllArgsConstructor
	@SuperBuilder
	private static class TestValue extends AbstractIncrementalAggregateRecord {
		private String value;
	}
}
