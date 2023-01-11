package io.kipe.streams.kafka.factories;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerdeFactory {

    private static final String JSON_POJO_CLASS = "JsonPOJOClass";
	@SuppressWarnings("rawtypes")
	private static final Map<Class, Serde> SERDES = new ConcurrentHashMap<>();

	private JsonSerdeFactory(){}

	public static synchronized <T> Serde<T> getJsonSerde(final Class<T> cls) {
		@SuppressWarnings("unchecked")
		Serde<T> serde = SERDES.get(cls);
		if(serde == null) {
			serde = createJSONSerde(cls);
			SERDES.put(cls, serde);
		}
		
		return serde;
	}
	
    private static <T> Serde<T> createJSONSerde(final Class<T> cls) {
        final Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<T> serializer = new JsonPOJOSerializer<>();
        serdeProps.put(JSON_POJO_CLASS, cls);
        serializer.configure(serdeProps, false);

        final Deserializer<T> deserializer = new JsonPOJODeserializer<>();
        serdeProps.put(JSON_POJO_CLASS, cls);
        deserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(serializer, deserializer);
    }
    
	// ------------------------------------------------------------------------
	// JsonPOJODeserializer
	// ------------------------------------------------------------------------

	private static class JsonPOJOSerializer<T> implements Serializer<T> {
	    private final ObjectMapper objectMapper = new ObjectMapper();

	    /**
	     * Default constructor needed by Kafka
	     */
	    JsonPOJOSerializer() {}
	    
	    @Override
	    public void configure(final Map<String, ?> props, final boolean isKey) {
	        // nothing to do
	    }

	    @Override
	    public byte[] serialize(final String topic, final T data) {
	        if (data == null)
	            return new byte[0];

	        try {
	            return objectMapper.writeValueAsBytes(data);
	        } catch (final Exception e) {
	            throw new SerializationException("Error serializing JSON message", e);
	        }
	    }

	    @Override
	    public void close() {
	        // nothing to do
	    }
	}
	
	// ------------------------------------------------------------------------
	// JsonPOJODeserializer
	// ------------------------------------------------------------------------

	private static class JsonPOJODeserializer<T> implements Deserializer<T> {
	    private final ObjectMapper objectMapper = new ObjectMapper();

	    private Class<T> tClass;

	    /**
	     * Default constructor needed by Kafka
	     */
	    JsonPOJODeserializer() {
	    }

	    @SuppressWarnings("unchecked")
	    @Override
	    public void configure(final Map<String, ?> props, final boolean isKey) {
	        tClass = (Class<T>) props.get(JSON_POJO_CLASS);
	    }

	    @Override
	    public T deserialize(final String topic, final byte[] bytes) {
	        if (bytes == null)
	            return null;

	        T data;
	        try {
	            data = objectMapper.readValue(bytes, tClass);
	        } catch (final Exception e) {
	            throw new SerializationException(e);
	        }

	        return data;
	    }

	    @Override
	    public void close() {
	        // nothing to do
	    }
	}
}
