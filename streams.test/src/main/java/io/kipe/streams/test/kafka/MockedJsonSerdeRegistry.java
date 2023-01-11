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
public class MockedJsonSerdeRegistry extends JsonSerdeRegistry {
	static JsonSerdeRegistry create() {
		return new MockedJsonSerdeRegistry(mock(BeanContext.class));
	}
	
	private final BeanContext beanContextMock;
	private final JsonObjectSerializer objectSerializer;
	
	public MockedJsonSerdeRegistry(BeanContext mock) {
		super(mock);
		
		this.beanContextMock = mock;
		this.objectSerializer = new JsonObjectSerializer(new JacksonDatabindMapper());
	}

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