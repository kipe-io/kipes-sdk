package io.kipe.streams.kafka.processors;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class Adsa {
    public static void main(String[] args) {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream("input-topic");

        DedupBuilder<String, String, String, String> dedupBuilder = new DedupBuilder<>(
                streamsBuilder,
                stream,
                Serdes.String(),
                Serdes.String(),
                "dedup-topic"
        );

        TopologyBuilder<String, String> deduped = dedupBuilder
                .groupBy((key, value) -> value, Serdes.String())
                .advanceBy((key, value) -> value)
                .emitFirst();
    }
}
