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
package io.kipe.streams.recordtypes;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

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
	 * Test complex object in GenericRecord field.
	 *
	 * @throws JsonProcessingException if a problem was encountered when processing.
	 */
	@Test
	void test_complex_object_in_field() throws JsonProcessingException {
		r.set(FIELD, new TestType(VALUE));

		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(r);
		
		GenericRecord serdeRecord = mapper.readValue(json, GenericRecord.class);
		assertEquals(r, serdeRecord);
		assertEquals(VALUE, ((TestType)r.get(FIELD)).getContent());
	}

	/**
	 * Test array, set, list, map of simple objects in GenericRecord field.
	 */
	@Test
	void test_collection_of_simple_objects_in_field() throws JsonProcessingException {
		Set<String> set = new HashSet<>(Arrays.asList("one", "two", "three"));
		List<String> list = Arrays.asList("one", "two", "three");
		String[] array = new String[]{"one", "two", "three"};

		Map<String, Integer> map = new HashMap<>();
		map.put("one", 1);
		map.put("two", 2);
		map.put("three", 3);

		r.set("listField", list);
		r.set("setField", set);
		r.set("arrayField", array);
		r.set("mapField", map);

		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(r);

		GenericRecord serdeRecord = mapper.readValue(json, GenericRecord.class);

		assertEquals(list, serdeRecord.get("listField"));
		assertEquals(set, serdeRecord.get("setField"));
		assertArrayEquals(array, serdeRecord.get("arrayField"));
		assertEquals(map, serdeRecord.get("mapField"));
	}

	/**
	 * Test collection of complex objects in GenericRecord field.
	 */
	@Test
	void test_collection_of_complex_objects_in_field() throws JsonProcessingException {
		List<TestType> complexObjects = Arrays.asList(
				new TestType("first"),
				new TestType("second"),
				new TestType("third")
		);

		r.set(FIELD, complexObjects);

		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(r);

		GenericRecord serdeRecord = mapper.readValue(json, GenericRecord.class);

		List<Map<String, String>> deserializedTypes = serdeRecord.get("field");

		for (int i = 0; i < complexObjects.size(); i++) {
			assertEquals(complexObjects.get(i).getContent(), deserializedTypes.get(i).get("content"));
		}

	}

	/**
	 * Test collection of collections in GenericRecord field.
	 */
	@Test
	void test_collection_of_collections_in_field() throws JsonProcessingException {
		List<String> innerList1 = Arrays.asList("one", "two");
		List<String> innerList2 = Arrays.asList("three", "four");
		List<String> innerList3 = Arrays.asList("five", "six");

		List<List<String>> listOfLists = new ArrayList<>();
		listOfLists.add(innerList1);
		listOfLists.add(innerList2);
		listOfLists.add(innerList3);

		r.set("listOfListsField", listOfLists);

		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(r);

		GenericRecord serdeRecord = mapper.readValue(json, GenericRecord.class);
		assertEquals(r, serdeRecord);

		List<List<String>> deserializedListOfLists = serdeRecord.get("listOfListsField");
		assertEquals(listOfLists.size(), deserializedListOfLists.size());
		for (int i = 0; i < listOfLists.size(); i++) {
			assertEquals(listOfLists.get(i), deserializedListOfLists.get(i));
		}
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
