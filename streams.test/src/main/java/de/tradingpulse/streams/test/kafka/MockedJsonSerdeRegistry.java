package de.tradingpulse.streams.test.kafka;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import org.apache.kafka.common.serialization.Serde;

import io.micronaut.configuration.kafka.serde.JsonSerde;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.context.BeanContext;
import io.micronaut.core.reflect.ClassUtils;

public class MockedJsonSerdeRegistry extends JsonSerdeRegistry {
	static JsonSerdeRegistry create() {
		return new MockedJsonSerdeRegistry(mock(BeanContext.class));
	}
	
	private final BeanContext beanContextMock;
	
	public MockedJsonSerdeRegistry(BeanContext mock) {
		super(mock);
		
		this.beanContextMock = mock;
	}

	@Override
	public <T> Serde<T> getSerde(Class<T> type) {
		if(ClassUtils.isJavaBasicType(type)) {
			return super.getSerde(type);
		}
		reset(beanContextMock);
		
		lenient()
		.when(beanContextMock.createBean(JsonSerde.class, type))
		.thenReturn(new JsonSerde<>(type));
		
		return super.getSerde(type);
	}
	
}