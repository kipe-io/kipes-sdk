package io.kipe.streams.kafka.examples.zomatorideranalysis.serdes;

import com.tdunning.math.stats.TDigest;
import org.apache.kafka.common.serialization.Serdes;

public class TDigestSerde extends Serdes.WrapperSerde<TDigest> {
    public TDigestSerde() {
        super(new TDigestSerializer(), new TDigestDeserializer());
    }
}
