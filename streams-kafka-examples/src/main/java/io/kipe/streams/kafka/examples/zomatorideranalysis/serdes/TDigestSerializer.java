package io.kipe.streams.kafka.examples.zomatorideranalysis.serdes;

import com.tdunning.math.stats.TDigest;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;

public class TDigestSerializer implements Serializer<TDigest> {

    @Override
    public byte[] serialize(String topic, TDigest data) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(data.byteSize());
            data.asBytes(buffer);
            return buffer.array();
        } catch (Exception e) {
            throw new SerializationException("Error serializing TDigest", e);
        }
    }
}
