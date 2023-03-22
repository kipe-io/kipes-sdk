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
package io.kipe.streams.kafka.serdes.deserializer;


// ------------------------------------------------------------------------
// JsonPOJODeserializer
// ------------------------------------------------------------------------

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kipe.streams.recordtypes.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * A private static class that implements the Deserializer interface. It is used to deserialize objects of type T
 * from a JSON format to a POJO format using the ObjectMapper class.
 * <p>
 * The class should be used with Kafka as it includes a default constructor that is needed by Kafka.
 */
public class GenericRecordDeserializer<T> implements Deserializer<GenericRecord> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public GenericRecordDeserializer() {
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
    }

    /**
     * Deserializes a JSON byte array to a POJO of type T.
     *
     * @param topic The topic the data is being deserialized from.
     * @param bytes The JSON byte array that will be deserialized.
     * @return The deserialized POJO of type T.
     */
    @Override
    public GenericRecord deserialize(final String topic, final byte[] bytes) {
        if (bytes == null) return null;

        GenericRecord data;
        try {
            data = objectMapper.readValue(bytes, GenericRecord.class);
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