package io.kipe.streams.recordtypes;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class GenericRecordTest4 {

    private GenericRecord r;

    @BeforeEach
    void beforeEach() {
        r = new GenericRecord();
    }

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

        // Hacky until we implement a correct serializer
        if (fieldValue == null) {
            assertEquals(r, serdeRecord);
        } else if (fieldValue.getClass().isArray()) {
            assertArrayEquals((Object[]) r.get(fieldName), serdeRecord.get(fieldName));
        } else if (fieldValue instanceof Set) {
            assertIterableEquals(r.get(fieldName), (serdeRecord.get(fieldName)));
        } else if (fieldValue instanceof Collection) {
            assertIterableEquals(r.get(fieldName), (serdeRecord.get(fieldName)));
        } else {
            assertEquals(r, serdeRecord);
        }
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

    // Optional will work if mapper is registered with Jdk8Module
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

        // equals and hashCode methods

        public class CustomSerializer extends JsonSerializer<NonSerializableCustomType> {
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
