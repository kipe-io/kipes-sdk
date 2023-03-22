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
package io.kipe.streams.kafka.factories;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.PrimitiveAvroSerde;
import io.confluent.kafka.streams.serdes.avro.ReflectionAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;

/**
 * Factory class for creating Kafka Avro Serde instances.
 * <p>
 * It requires access to a schema registry for schema resolution.
 */
public class AvroSerdeFactory {

    private AvroSerdeFactory() {
        throw new AssertionError("Cannot instantiate AvroSerdeFactory");
    }

    /**
     * Creates a {@link GenericAvroSerde} instance.
     *
     * @param serdeConfig the configuration map for the Serde.
     * @param isKey       true if the Serde is for a key, false otherwise.
     * @return a new GenericAvroSerde instance.
     */
    public static GenericAvroSerde createGenericAvroSerde(Map<String, ?> serdeConfig, boolean isKey) {
        final GenericAvroSerde serde = new GenericAvroSerde();
        serde.configure(serdeConfig, isKey);
        return serde;
    }

    /**
     * Creates a {@link SpecificAvroSerde} instance.
     *
     * @param serdeConfig the configuration map for the Serde.
     * @param isKey       true if the Serde is for a key, false otherwise.
     * @param <T>         the type of the Serde, which must extend {@link SpecificRecord}.
     * @return a new SpecificAvroSerde instance.
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> createSpecificAvroSerde(Map<String, ?> serdeConfig, boolean isKey) {
        final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, isKey);
        return serde;
    }

    /**
     * Creates a {@link PrimitiveAvroSerde} instance.
     *
     * @param serdeConfig the configuration map for the Serde.
     * @param isKey       true if the Serde is for a key, false otherwise.
     * @param <T>         the type of the Serde.
     * @return a new PrimitiveAvroSerde instance.
     */
    public static <T> PrimitiveAvroSerde<T> createPrimitiveAvroSerde(Map<String, ?> serdeConfig, boolean isKey) {
        final PrimitiveAvroSerde<T> serde = new PrimitiveAvroSerde<>();
        serde.configure(serdeConfig, isKey);
        return serde;
    }

    /**
     * Creates a {@link ReflectionAvroSerde} instance.
     *
     * @param serdeConfig the configuration map.
     * @param isKey       whether the serde is for a key or a value.
     * @param <T>         the type of the object that the serde serializes and deserializes.
     * @return a configured {@link ReflectionAvroSerde} instance.
     */
    public static <T> ReflectionAvroSerde<T> createReflectionAvroSerde(Map<String, ?> serdeConfig, boolean isKey) {
        final ReflectionAvroSerde<T> serde = new ReflectionAvroSerde<>();
        serde.configure(serdeConfig, isKey);
        return serde;
    }
}
