/*
 * Kipes SDK Test Extensions for Kafka - The High-Level Event Processing SDK.
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
package io.kipe.streams.test.kafka;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import org.apache.kafka.common.serialization.Serde;

import io.micronaut.configuration.kafka.serde.JsonObjectSerde;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.context.BeanContext;
import io.micronaut.core.reflect.ClassUtils;
import io.micronaut.jackson.databind.JacksonDatabindMapper;
import io.micronaut.json.JsonObjectSerializer;

// TODO documentation

/**
 * MockedJsonSerdeRegistry is a mock implementation of the {@link JsonSerdeRegistry} class.
 */
public class MockedJsonSerdeRegistry extends JsonSerdeRegistry {
	/**
	 * Creates a new instance of MockedJsonSerdeRegistry
	 *
	 * @return new instance of MockedJsonSerdeRegistry
	 */
	static JsonSerdeRegistry create() {
		return new MockedJsonSerdeRegistry(mock(BeanContext.class));
	}
	
	private final BeanContext beanContextMock;
	private final JsonObjectSerializer objectSerializer;

	/**
	 * Creates a new MockedJsonSerdeRegistry instance
	 * @param mock a mock BeanContext
	 */
	public MockedJsonSerdeRegistry(BeanContext mock) {
		super(mock);
		
		this.beanContextMock = mock;
		this.objectSerializer = new JsonObjectSerializer(new JacksonDatabindMapper());
	}

	/**
	 * Overrides the {@link JsonSerdeRegistry#getSerde(Class)} method to return a new {@link JsonObjectSerde}
	 * instance with a mocked BeanContext
	 *
	 * @param type the class of the object that the serde will be used for.
	 * @return new {@link JsonObjectSerde} instance
	 */
	@Override
	public <T> Serde<T> getSerde(Class<T> type) {
		if(ClassUtils.isJavaBasicType(type)) {
			return super.getSerde(type);
		}
		reset(beanContextMock);
		
		lenient()
		.when(beanContextMock.createBean(JsonObjectSerde.class, type))
		.thenReturn(new JsonObjectSerde<T>(this.objectSerializer, type));
		
		return super.getSerde(type);
	}
	
}