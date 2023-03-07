package io.kipe.streams.kafka.serdes;

import io.kipe.streams.kafka.serdes.deserializer.TestRecordSequenceDeserializer;
import io.kipe.streams.kafka.serdes.serializer.JsonSerializer;
import io.kipe.streams.recordtypes.TestRecordSequence;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class TestRecordSequenceSerdes implements Serde<TestRecordSequence> {
    private final JsonSerializer<TestRecordSequence> serializer = new JsonSerializer<>();
    private final TestRecordSequenceDeserializer<TestRecordSequence> deserializer = new TestRecordSequenceDeserializer<>();

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
    public Serializer<TestRecordSequence> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<TestRecordSequence> deserializer() {
        return deserializer;
    }
}