package io.kipe.streams.kafka.factories;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.kipe.streams.kafka.factories.JsonSerdeFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Test class for {@link JsonSerdeFactory}.
 */
class JsonSerdeFactoryTest {

	/**
	 * Test for serde with generic class without generics
	 */
	@Test
	void test_serde__generic_class_without_generics() {
		GenericClass<String> gco = new GenericClass<>("value");
		
		@SuppressWarnings("rawtypes")
		Serde<GenericClass> serde = JsonSerdeFactory.getJsonSerde(GenericClass.class);
		
		byte[] json = serde.serializer().serialize("topic", gco);
		
		@SuppressWarnings( "unchecked" )
		GenericClass<String> deseGco = serde.deserializer().deserialize("topic", json);

		assertEquals(gco, deseGco);
		assertEquals(gco.getValue(), deseGco.getValue());
	}

	/**
	 * Test for serde with generic class with generics
	 */
	@Test
	void test_serde__generic_class_with_generics() {
		GenericClass<String> gco = new GenericClass<>("value");
		
		@SuppressWarnings("unchecked")
		Serde<GenericClass<String>> serde = JsonSerdeFactory.getJsonSerde((Class<GenericClass<String>>)(Class<?>)GenericClass.class);
		
		byte[] json = serde.serializer().serialize("topic", gco);
		
		GenericClass<String> deseGco = serde.deserializer().deserialize("topic", json);

		assertEquals(gco, deseGco);
		assertEquals(gco.getValue(), deseGco.getValue());
	}

	// ------------------------------------------------------------------------
	// GenericClass
	// ------------------------------------------------------------------------

	/**
	 * Data class for testing {@link JsonSerdeFactory}
	 *
	 * @param <T>
	 */
	@Data
	@EqualsAndHashCode
	@NoArgsConstructor
	@AllArgsConstructor
	private static class GenericClass<T> {

		/**
		 * Annotation for handling class information during deserialization.
		 */
		@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
		private T value;
	}
}
