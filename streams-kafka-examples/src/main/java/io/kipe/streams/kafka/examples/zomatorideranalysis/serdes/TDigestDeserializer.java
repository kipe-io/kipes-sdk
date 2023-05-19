package io.kipe.streams.kafka.examples.zomatorideranalysis.serdes;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;

public class TDigestDeserializer implements Deserializer<TDigest> {

    @Override
    public TDigest deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);
            return MergingDigest.fromBytes(buffer);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing TDigest", e);
        }
    }
}

