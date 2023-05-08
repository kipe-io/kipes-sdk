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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JsonSerdeFactory is a utility class for creating Serde instances for JSON
 * serialization and deserialization using Jackson's ObjectMapper.
 * It creates a new Serde instance for a given POJO class, if one doesn't exist
 * already, and caches it for future use. This ensures that a single Serde
 * instance is used for all instances of the same POJO class, reducing memory
 * usage and improving performance.
 */
public class JsonSerdeFactory {

    private static final String JSON_POJO_CLASS = "JsonPOJOClass";
	@SuppressWarnings("rawtypes")
	private static final Map<Class, Serde> SERDES = new ConcurrentHashMap<>();

	/**
	 * Private constructor to prevent instantiation of this utility class.
	 */
	private JsonSerdeFactory(){}

	/**
	 * Returns a Serde instance for the given POJO class that can be used for
	 * serializing and deserializing JSON using Jackson's ObjectMapper.
	 * <p>
	 * If a Serde instance for the same class has already been created, it will
	 * be returned from the cache. Otherwise, a new instance will be created and
	 * cached for future use.
	 *
	 * @param cls The POJO class for which the Serde instance should be created.
	 * @return The Serde instance for the given POJO class.
	 */
	public static synchronized <T> Serde<T> getJsonSerde(final Class<T> cls) {
		@SuppressWarnings("unchecked")
		Serde<T> serde = SERDES.get(cls);
		if(serde == null) {
			serde = createJSONSerde(cls);
			SERDES.put(cls, serde);
		}
		
		return serde;
	}

	/**
	 * Creates a new Serde instance for the given POJO class using the
	 * JsonPOJOSerializer and JsonPOJODeserializer classes.
	 *
	 * @param cls The POJO class for which the Serde instance should be created.
	 * @return The Serde instance for the given POJO class.
	 */
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

	/**
	 * JsonPOJOSerializer is a Kafka {@link Serializer} that uses Jackson's {@link ObjectMapper} to serialize objects to JSON.
	 * <p>A default constructor is provided to make this class usable by Kafka. The serializer can be configured
	 * with a map of properties, but this implementation ignores any provided properties.
	 * <p>The {@link #serialize(String, Object)} method is used by Kafka to convert an object to a byte array.
	 * If the provided object is null, an empty byte array is returned. If there is an error serializing the object
	 * to JSON, a {@link SerializationException} is thrown.
	 * <p>The {@link #close()} method is a no-op in this implementation.
	 */
	private static class JsonPOJOSerializer<T> implements Serializer<T> {
	    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

	    /**
	     * Default constructor needed by Kafka
	     */
	    JsonPOJOSerializer() {}

		/**
		 * Configure this class.
		 *
		 * @param props map of properties used to configure this class. Ignored in this implementation.
		 * @param isKey whether the serializer is being used for a key or value. Ignored in this implementation.
		 */
	    @Override
	    public void configure(final Map<String, ?> props, final boolean isKey) {
	        // nothing to do
	    }

		/**
		 * Serialize the provided object to a byte array.
		 *
		 * @param topic the topic associated with the data. Ignored in this implementation.
		 * @param data  the object to serialize.
		 * @return a byte array containing the serialized JSON, or an empty byte array if the provided object is null.
		 * @throws SerializationException if there is an error serializing the object to JSON.
		 */
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

		/**
		 * Close this serializer.
		 * <p>
		 * This implementation is a no-op.
		 */
	    @Override
	    public void close() {
	        // nothing to do
	    }
	}
	
	// ------------------------------------------------------------------------
	// JsonPOJODeserializer
	// ------------------------------------------------------------------------

	/**
	 * A private static class that implements the Deserializer interface. It is used to deserialize objects of type T
	 * from a JSON format to a POJO format using the ObjectMapper class.
	 * <p>
	 * The class should be used with Kafka as it includes a default constructor that is needed by Kafka.
	 */
	private static class JsonPOJODeserializer<T> implements Deserializer<T> {
	    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

	    private Class<T> tClass;

	    /**
	     * Default constructor needed by Kafka
	     */
	    JsonPOJODeserializer() {
	    }

		/**
		 * Configures the deserializer with the class of the POJO that it will deserialize to.
		 *
		 * @param props A map of properties that can be used to configure the deserializer.
		 * @param isKey A boolean value indicating whether the deserializer is being used to deserialize keys or values.
		 */
	    @SuppressWarnings("unchecked")
	    @Override
	    public void configure(final Map<String, ?> props, final boolean isKey) {
	        tClass = (Class<T>) props.get(JSON_POJO_CLASS);
	    }

		/**
		 * Deserializes a JSON byte array to a POJO of type T.
		 *
		 * @param topic The topic the data is being deserialized from.
		 * @param bytes The JSON byte array that will be deserialized.
		 * @return The deserialized POJO of type T.
		 */
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

		/**
		 * This method is called when the deserializer is closed. It can be used to free up any resources.
		 */
	    @Override
	    public void close() {
	        // nothing to do
	    }
	}
}
