package io.kipe.streams.recordtypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kipe.streams.recordtypes.TransactionRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Test class for {@link TransactionRecord}.
 */
class TransactionRecordTest {

	private static final String GROUP_KEY = "groupKey";
	private static final String VALUE = "value";

//	// ------------------------------------------------------------------------
//	// tests createFrom
//	// ------------------------------------------------------------------------
//
//	@Test
//	void test_createFrom__correctly_initializes() {
//		TestValue value = createTestValue(VALUE);
//		
//		TransactionRecord<Void, TestValue> record = TransactionRecord.createFrom(value);
//		
//		assertNotNull(value.getKey());
//		assertEquals(value.getKey(), record.getKey());
//		assertFalse(record.getRecords().contains(value));
//	}

	// ------------------------------------------------------------------------
	// tests addUnique
	// ------------------------------------------------------------------------

//	@Test
//	void test_addUnique__fails_on_this_key_null() {
//		TransactionRecord<Void, TestValue> record = new TransactionRecord<>();
//		TestValue value = createTestValue(VALUE);
//		
//		assertThrows(NullPointerException.class, () -> {
//			record.addUnique(value);
//		});
//	}

	/**
	 * Test that {@link TransactionRecord#addUnique(Object)} throws a {@link NullPointerException} when the value is null.
	 */
	@Test
	void test_addUnique__fails_on_value_null() {
		TransactionRecord<Void, TestValue> record = createTransactionRecord();

		assertThrows(NullPointerException.class, () -> {
			record.addUnique(null);
		});
	}

//	@Test
//	void test_addUnique__fails_on_value_key_null() {
//		TransactionRecord<Void, TestValue> record = createTransactionRecord();
//		TestValue value = new TestValue();
//		
//		assertThrows(NullPointerException.class, () -> {
//			record.addUnique(value);
//		});
//	}

	/**
	 * Test that {@link TransactionRecord#addUnique(Object)} adds a value to the records.
	 */
	@Test
	void test_addUnique__adds_value() {
		TransactionRecord<Void, TestValue> record = createTransactionRecord();
		TestValue value = createTestValue(VALUE);

		record.addUnique(value);

		assertTrue(record.getRecords().contains(value));
	}

	/**
	 * Test that {@link TransactionRecord#addUnique(Object)} ignores adding already stored values.
	 */
	@Test
	void test_addUnique__ignores_adding_already_stored_values() {
		TransactionRecord<Void, TestValue> record = createTransactionRecord();
		TestValue value = createTestValue(VALUE);

		record.addUnique(value);
		record.addUnique(value);

		assertEquals(1, record.getRecords().size());
	}

//	@Test
//	void test_addUnique__updates_this_timestamp() {
//		TransactionRecord<Void, TestValue> record = createTransactionRecord();
//		long thisTimestamp = record.getKey().getTimestamp();
//		
//		await().atMost(Duration.ofSeconds(1)).until(() -> System.currentTimeMillis() > thisTimestamp);
//		TestValue value = createTestValue(VALUE);
//		
//		record.addUnique(value);
//		
//		assertTrue(thisTimestamp < record.getKey().getTimestamp());
//		assertEquals(record.getKey().getTimestamp(), value.getKey().getTimestamp());
//	}

	// ------------------------------------------------------------------------
	// tests getRecord
	// ------------------------------------------------------------------------

	/**
	 * Test that {@link TransactionRecord#getRecords()} throws a IndexOutOfBoundsException when accessed index is out of bounds.
	 */
	@Test
	void test_getRecord__throws_IndexOutOfBoundsException() {
		TransactionRecord<Void, TestValue> record = createTransactionRecord();

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

	/**
	 * Test that {@link TransactionRecord#getRecords()} returns the correct value for a given index.
	 */
	@Test
	void test_getRecord__returns_correct_record() {
		TransactionRecord<Void, TestValue> record = createTransactionRecord();

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

    /**
     * Test for the serialization and deserialization of the {@link TransactionRecord} class.
     */
	@Test
	void test_serde() throws JsonProcessingException {
		TransactionRecord<String, TestValue> record = createTransactionRecord();
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

	/**
	 * Create a new instance of {@link TransactionRecord} with generic key and value types.
	 *
	 * @param <GK> the generic type for the key
	 * @param <V>  the generic type for the value
	 * @return a new instance of {@link TransactionRecord}
	 */
	private static <GK,V> TransactionRecord<GK, V> createTransactionRecord() {
		return new TransactionRecord<GK,V>();
	}

	/**
	 * Create a new instance of {@link TestValue} with the given value.
	 *
	 * @param value the value to be set in the {@link TestValue} instance.
	 * @return a new instance of {@link TestValue}.
	 */
	private static TestValue createTestValue(String value) {
		return new TestValue(value);
	}

	// ------------------------------------------------------------------------
	// inner class - TestValue
	// ------------------------------------------------------------------------

    /**
     * A simple value object used for testing.
     */
	@Data
	@EqualsAndHashCode
	@ToString(callSuper = true)
	@NoArgsConstructor
	@AllArgsConstructor
	private static class TestValue {
		private String value;
	}
}
