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

import com.google.protobuf.Message;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;

import java.util.Map;

/**
 * Factory class for creating Kafka Protobuf Serde instances.
 * <p>
 * It requires access to a schema registry for schema resolution.
 */
public class ProtobufSerdeFactory {

    private ProtobufSerdeFactory() {
        throw new AssertionError("Cannot instantiate ProtobufSerdeFactory");
    }

    /**
     * Creates a {@link KafkaProtobufSerde} instance.
     *
     * @param serdeConfig the configuration map.
     * @param isKey       whether the serde is for a key or a value.
     * @param <T>         the type of the object that the serde serializes and deserializes.
     * @return a configured {@link KafkaProtobufSerde} instance.
     */
    public static <T extends Message> KafkaProtobufSerde<T> createProtoSerde(Map<String, ?> serdeConfig, boolean isKey) {
        final KafkaProtobufSerde<T> serde = new KafkaProtobufSerde<>();
        serde.configure(serdeConfig, isKey);
        return serde;
    }
}
