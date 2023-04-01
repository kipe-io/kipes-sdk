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
package io.kipe.streams.kafka.factories;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

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
