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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JsonSerdeFactory is a utility class for creating Serde instances for JSON serialization and deserialization using
 * Jackson's ObjectMapper. It caches Serde instances for each POJO class, reducing memory usage and improving
 * performance.
 */
public class JsonSerdeFactory {
    private static final Map<Class<?>, Serde<?>> SERDES = new ConcurrentHashMap<>();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonSerdeFactory() {
    }

    /**
     * Registers one or more Jackson modules to the ObjectMapper.
     *
     * @param modules The modules to register.
     */
    public static void registerModules(Module... modules) {
        OBJECT_MAPPER.registerModules(modules);
    }

    /**
     * Returns a Serde instance for the given POJO class for JSON serialization and deserialization.
     *
     * @param cls The POJO class for which the Serde instance should be created.
     * @return The Serde instance for the given POJO class.
     */
    @SuppressWarnings("unchecked")
    public static <T> Serde<T> getJsonSerde(Class<T> cls) {
        return (Serde<T>) SERDES.computeIfAbsent(cls, JsonSerdeFactory::createJsonSerde);
    }

    private static <T> Serde<T> createJsonSerde(Class<T> cls) {
        Serializer<T> serializer = new JsonPOJOSerializer<>(cls);
        Deserializer<T> deserializer = new JsonPOJODeserializer<>(cls);

        return Serdes.serdeFrom(serializer, deserializer);
    }

    // ------------------------------------------------------------------------
    // JsonPOJOSerializer
    // ------------------------------------------------------------------------

    /**
     * JsonPOJOSerializer is a Kafka Serializer that uses Jackson's ObjectMapper for JSON serialization.
     */
    private static class JsonPOJOSerializer<T> implements Serializer<T> {
        private final Class<T> cls;

        JsonPOJOSerializer(Class<T> cls) {
            this.cls = cls;
        }

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) {
                return new byte[0];
            }

            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
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


    /**
     * JsonPOJODeserializer is a Kafka Deserializer that uses Jackson's ObjectMapper for JSON deserialization.
     */
    private static class JsonPOJODeserializer<T> implements Deserializer<T> {
        private final Class<T> cls;

        JsonPOJODeserializer(Class<T> cls) {
            this.cls = cls;
        }

        @Override
        public T deserialize(String topic, byte[] bytes) {
            if (bytes == null) {
                return null;
            }

            try {
                return OBJECT_MAPPER.readValue(bytes, cls);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public void close() {
            // nothing to do
        }
    }
}
