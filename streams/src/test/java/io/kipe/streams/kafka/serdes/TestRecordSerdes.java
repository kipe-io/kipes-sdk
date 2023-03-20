package io.kipe.streams.kafka.serdes;

import io.kipe.streams.kafka.serdes.deserializer.TestRecordDeserializer;
import io.kipe.streams.kafka.serdes.serializer.JsonSerializer;
import io.kipe.streams.recordtypes.TestRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class TestRecordSerdes implements Serde<TestRecord> {
    private final JsonSerializer<TestRecord> serializer = new JsonSerializer<>();
    private final TestRecordDeserializer<TestRecord> deserializer = new TestRecordDeserializer<>();

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
    public Serializer<TestRecord> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<TestRecord> deserializer() {
        return deserializer;
    }
}