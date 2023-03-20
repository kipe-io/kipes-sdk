package io.kipe.streams.recordtypes;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kipe.streams.recordtypes.GenericRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


///**
// * Test class for {@link GenericRecord}. This class includes test cases for the following functionality:
// * <ul>
// * <li>{@link GenericRecord#withNewFieldsFrom(GenericRecord)}</li>
// * <p>
// * <li>{@link GenericRecord#set(String, Object)} and {@link GenericRecord#get(String)}</li>
// * <p>
// * <li>{@link GenericRecord#remove(String)}</li>
// * <p>
// * <li>Serialization and Deserialization (serde) using Jackson</li>
// *
// * </ul>

/**
 * Test class for {@link GenericRecord}.
 */
class GenericRecordTest {
	private static final String FIELD = "field";
	private static final String VALUE = "value";
	private static final String OTHER_VALUE = "otherValue";
	
	private GenericRecord r;

	/**
	 * Initializes the test instance before each test.
	 */
	@BeforeEach
	void beforeEach() {
		r = new GenericRecord();
	}
	
	// ------------------------------------------------------------------------
	// tests withNewFieldsFrom
	// ------------------------------------------------------------------------

	/**
	 * Test {@link GenericRecord#withNewFieldsFrom(GenericRecord)} method.
	 */
	@Test
	void test_withNewFieldsFrom() {
		r.with(FIELD, VALUE);
		r.withNewFieldsFrom(GenericRecord.create()
				.with(FIELD, OTHER_VALUE)		// will be ignored
				.with("otherField", "new"));	// will be added
		
		assertEquals(VALUE, r.get(FIELD));
		assertEquals("new", r.get("otherField"));
	}
	
	// ------------------------------------------------------------------------
	// tests set/get/remove
	// ------------------------------------------------------------------------

	/**
	 * Test {@link GenericRecord#get(String)} method with unknown field.
	 */
	@Test
	void test_get_unknown_field() {
		assertNull(r.get(FIELD));
	}

	/**
	 * Test {@link GenericRecord#get(String)} method returns the current value.
	 */
	@Test
	void test_get_returns_the_current_value() {
		r.set(FIELD, VALUE);		
		assertEquals(VALUE, r.get(FIELD));
		
		r.set(FIELD, OTHER_VALUE);
		assertEquals(OTHER_VALUE, r.get(FIELD));
	}

	/**
	 * Test {@link GenericRecord#remove(String)} method removes the field.
	 */
	@Test
	void test_remove_removes() {
		r.set(FIELD, VALUE);		
		r.remove(FIELD);
		assertNull(r.get(FIELD));
	}

	/**
	 * Test {@link GenericRecord#set(String, Object)} method with null value removes the field.
	 */
	@Test
	void test_set_null_removes() {
		r.set(FIELD, VALUE);		
		r.set(FIELD, null);
		assertNull(r.get(FIELD));
	}
	
	// ------------------------------------------------------------------------
	// serde
	// ------------------------------------------------------------------------

	/**
	 * Test {@link GenericRecord} is serdeable.
	 *
	 * @throws JsonProcessingException if a problem was encountered when processing.
	 */
	@Test
	void test_is_serdeable() throws JsonProcessingException {
		r.set(FIELD, new TestType(VALUE));

		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(r);
		
		GenericRecord serdeRecord = mapper.readValue(json, GenericRecord.class);
		assertEquals(r, serdeRecord);
		assertEquals(VALUE, ((TestType)r.get(FIELD)).getContent());
	}
	
	// ------------------------------------------------------------------------
	// field classes
	// ------------------------------------------------------------------------

	/**
	 * A simple POJO used for testing serialization/deserialization of {@link GenericRecord}.
	 */
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TestType {
		
		private String content;
	}
}
