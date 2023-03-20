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
