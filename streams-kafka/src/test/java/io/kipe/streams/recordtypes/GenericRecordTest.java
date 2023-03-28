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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

/**g
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

	private static Stream<Arguments> dataProvider() {
		return Stream.of(
				// Basic data types
				arguments("stringValue", "Hello, Kipes!"),
				arguments("intValue", 42),
				arguments("doubleValue", 3.14),
				arguments("booleanValue", true),

				// Custom data type
				arguments("customTypeValue", new GenericRecordTest.TestType("customContent")),

				// Collection data types
				arguments("listValue", List.of("one", "two", "three")),
				arguments("mapValue", Map.of("one", 1, "two", 2, "three", 3)),
				arguments("emptyListValue", Collections.EMPTY_LIST),
				arguments("emptyMapValue", Collections.EMPTY_MAP),
				arguments("nestedListValue", List.of(List.of("one", "two"), List.of("three", "four"))),
				arguments("nestedMapValue", Map.of("first", Map.of("one", 1, "two", 2), "second", Map.of("three", 3, "four", 4))),
				arguments("stringListValue", List.of("one", "two", "three")),
				arguments("intMapValue", Map.of("one", 1, "two", 2, "three", 3)),
				arguments("setOfMapsValue", Set.of(Map.of("one", 1), Map.of("two", 2))),
				arguments("listOfSetsValue", List.of(Set.of("one", "two"), Set.of("three", "four"))),
				arguments("setOfSetsValue", Set.of(Set.of("one", "two"), Set.of("three", "four"))),
				arguments("mapOfListsValue", Map.of("first", List.of("one", "two"), "second", List.of("three", "four"))),

				// Array data types
				arguments("intArrayValue", new Integer[]{1, 2, 3}),
				arguments("doubleArrayValue", new Double[]{1.0, 2.0, 3.0}),
				arguments("booleanArrayValue", new Boolean[]{true, false, true}),
				arguments("stringArrayValue", new String[]{"hello", "world"}),
				arguments("objectArrayValue", new Object[]{"hello", 42, true}),
				arguments("intArray2DValue", new Integer[][]{{1, 2}, {3, 4}}),
				arguments("objectArray3DValue", new Object[][][]{{{"one", "two"}, {"three", "four"}}, {{"five", "six"}, {"seven", "eight"}}}),

				// Mixed data types
				arguments("genericListValue", List.of("hello", 42, true)),
				arguments("genericMapValue", Map.of("one", "hello", "two", 42, "three", true)),
				arguments("nestedGenericListValue", List.of(List.of("one", 1), List.of(true, 3.14))),
				arguments("customParameterizedValue", new CustomParameterizedType<>("customParameterizedContent", Map.of("one", 1, "two", 2))),
				arguments("customNestedParameterizedValue", new CustomNestedParameterizedType<>(List.of(Map.of("one", 1, "two", 2), Map.of("three", 3, "four", 4)), Set.of("hello", "world"))),

				// Empty record
				arguments("emptyRecordValue", new GenericRecord()),

				// Null
				arguments("nullValue", null)
		);
	}
    @ParameterizedTest
    @MethodSource("dataProvider")
    void test_values_in_generic_record(String fieldName, Object fieldValue) throws JsonProcessingException {
        r.set(fieldName, fieldValue);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(r);

        GenericRecord serdeRecord = mapper.readValue(json, GenericRecord.class);

        // This temporary approach demonstrates field equality as we cannot always compare GenericRecords directly.
        // Unordered sets makes this challenging and requires workarounds.

        if (fieldValue == null) {
            assertEquals(r, serdeRecord);
        } else {
            if (fieldValue.getClass().isArray()) {
                assertArrayEquals(r.get(fieldName), (Object[]) serdeRecord.get(fieldName));
            } else if (fieldValue instanceof Set) {
                Object sortedRFieldValue = sortNestedCollections(r.get(fieldName));
                Object sortedSerdeRecordFieldValue = sortNestedCollections(serdeRecord.get(fieldName));
                assertIterableEquals(Collections.singleton(sortedRFieldValue), Collections.singleton(sortedSerdeRecordFieldValue));
            } else if (fieldValue instanceof Collection) {
                assertIterableEquals(Collections.singleton(r.get(fieldName)), Collections.singleton(serdeRecord.get(fieldName)));
            } else {
                assertEquals(r, serdeRecord);
            }
        }
    }

    private Object sortNestedCollections(Object collection) {
        if (collection instanceof List) {
            List<?> list = (List<?>) collection;
            List<Object> sortedList = new ArrayList<>(list);
            sortedList.sort(Comparator.comparing(Object::toString));
            sortedList.replaceAll(this::sortNestedCollections);
            return sortedList;
        } else if (collection instanceof Set) {
            Set<?> set = (Set<?>) collection;
            List<Object> sortedList = new ArrayList<>(set);
            sortedList.sort(Comparator.comparing(Object::toString));
            sortedList.replaceAll(this::sortNestedCollections);
            return new LinkedHashSet<>(sortedList);
        } else if (collection instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) collection;
            Map<Object, Object> sortedMap = new LinkedHashMap<>();
            map.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey(Comparator.comparing(Object::toString)))
                    .forEach(entry -> sortedMap.put(entry.getKey(), sortNestedCollections(entry.getValue())));
            return sortedMap;
        }
        return collection;
    }

	@Test
	void testListWithCyclicReferencesValue() {
		List<Object> listWithCyclicReferences = new ArrayList<>();
		listWithCyclicReferences.add("one");
		listWithCyclicReferences.add(listWithCyclicReferences);

		r.set("listWithCyclicReferencesValue", listWithCyclicReferences);

		ObjectMapper mapper = new ObjectMapper();

		assertThrows(JsonProcessingException.class, () -> {
			String json = mapper.writeValueAsString(r);
		});
	}

	// Note: The Jdk8Module is required to properly handle Optional fields in a GenericRecord with the ObjectMapper.
	// However, at the moment, Kipes does not support this module out of the box.
	@Test
	void testListWithOptionalValue() throws JsonProcessingException {
		Optional<String> optionalValue = Optional.of("value");
		r.set("optionalValue", optionalValue);

		ObjectMapper mapper = new ObjectMapper();

		String json = mapper.writeValueAsString(r);

		assertThrows(UnrecognizedPropertyException.class, () -> {
			GenericRecord serdeRecord = mapper.readValue(json, GenericRecord.class);
		});
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

	@Test
	void testNonSerializableCustomType() {
		NonSerializableCustomType customType = new NonSerializableCustomType("customData");
		r.set("nonSerializableCustomType", customType);

		ObjectMapper mapper = new ObjectMapper();

		assertThrows(JsonProcessingException.class, () -> {
			String json = mapper.writeValueAsString(r);
		});
	}

	@JsonSerialize(using = NonSerializableCustomType.CustomSerializer.class)
	public static class NonSerializableCustomType {
		private final String customData;

		public NonSerializableCustomType(String customData) {
			this.customData = customData;
		}

		public String getCustomData() {
			return customData;
		}

		public static class CustomSerializer extends JsonSerializer<NonSerializableCustomType> {
			@Override
			public void serialize(NonSerializableCustomType value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
				throw new IOException("Cannot serialize NonSerializableCustomType");
			}
		}
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CustomParameterizedType<T, U> {
		private T content1;
		private U content2;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CustomNestedParameterizedType<T, U> {
		private List<T> listContent;
		private Set<U> setContent;
	}
}
