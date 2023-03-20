package io.kipe.streams.kafka.serdes;

import io.kipe.streams.kafka.serdes.deserializer.GenericRecordDeserializer;
import io.kipe.streams.kafka.serdes.serializer.JsonSerializer;
import io.kipe.streams.recordtypes.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class GenericRecordSerdes implements Serde<GenericRecord> {
    private final JsonSerializer<GenericRecord> serializer = new JsonSerializer<>();
    private final GenericRecordDeserializer<GenericRecord> deserializer = new GenericRecordDeserializer<>();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<GenericRecord> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<GenericRecord> deserializer() {
        return deserializer;
    }
}