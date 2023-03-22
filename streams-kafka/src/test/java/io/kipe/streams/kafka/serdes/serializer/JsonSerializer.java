/*
 * Kipe Streams Kafka - Kipe Streams SDK
 * Copyright © 2023 Kipe.io
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
package io.kipe.streams.kafka.serdes.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * JsonSerializer is a Kafka {@link Serializer} that uses Jackson's {@link ObjectMapper} to serialize objects to JSON.
 * <p>A default constructor is provided to make this class usable by Kafka. The serializer can be configured
 * with a map of properties, but this implementation ignores any provided properties.
 * <p>The {@link #serialize(String, Object)} method is used by Kafka to convert an object to a byte array.
 * If the provided object is null, an empty byte array is returned. If there is an error serializing the object
 * to JSON, a {@link SerializationException} is thrown.
 * <p>The {@link #close()} method is a no-op in this implementation.
 */
public class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonSerializer() {
    }

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
