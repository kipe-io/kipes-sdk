package io.kipe.streams.kafka.serdes.deserializer;


// ------------------------------------------------------------------------
// JsonPOJODeserializer
// ------------------------------------------------------------------------

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kipe.streams.recordtypes.TestRecordSequence;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * A private static class that implements the Deserializer interface. It is used to deserialize objects of type T
 * from a JSON format to a POJO format using the ObjectMapper class.
 * <p>
 * The class should be used with Kafka as it includes a default constructor that is needed by Kafka.
 */
public class TestRecordSequenceDeserializer<T> implements Deserializer<TestRecordSequence> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public TestRecordSequenceDeserializer() {
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
    public TestRecordSequence deserialize(final String topic, final byte[] bytes) {
        if (bytes == null) return null;

        TestRecordSequence data;
        try {
            data = objectMapper.readValue(bytes, TestRecordSequence.class);
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