package io.kipe.streams.kafka.factories;

import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;

import java.util.Map;

/**
 * Factory class for creating Kafka Json Schema Serde instances.
 * <p>
 * It requires access to a schema registry for schema resolution.
 */
public class JsonSchemaSerdeFactory {

    private JsonSchemaSerdeFactory() {
        throw new AssertionError("Cannot instantiate JsonSchemaSerdeFactory");
    }

    /**
     * Creates a {@link KafkaJsonSchemaSerde} instance.
     *
     * @param serdeConfig the configuration map.
     * @param isKey       whether the serde is for a key or a value.
     * @param <T>         the type of the object that the serde serializes and deserializes.
     * @return a configured {@link KafkaJsonSchemaSerde} instance.
     */
    public static <T> KafkaJsonSchemaSerde<T> createJsonSchemaSerde(Map<String, ?> serdeConfig, boolean isKey) {
        final KafkaJsonSchemaSerde<T> serde = new KafkaJsonSchemaSerde<>();
        serde.configure(serdeConfig, isKey);
        return serde;
    }
}
