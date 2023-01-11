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

class JsonSerdeFactoryTest {

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

	@Data
	@EqualsAndHashCode
	@NoArgsConstructor
	@AllArgsConstructor
	private static class GenericClass<T> {
		
		@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
		private T value;
	}
}
