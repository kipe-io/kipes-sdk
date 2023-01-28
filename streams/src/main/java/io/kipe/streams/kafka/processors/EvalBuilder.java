package io.kipe.streams.kafka.processors;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import io.kipe.streams.recordtypes.GenericRecord;

/**
 * EvalBuilder is a class for building Kafka Stream topologies that update
 * fields in GenericRecord value based on expressions provided to the with() method.
 * <p>
 * <b>Usage:</b>
 * <p>
 * TODO
 *
 * <b>Example:</b>
 * <pre>
 * {@code
 *   StreamsBuilder builder = new StreamsBuilder();
 *   KStream<String, GenericRecord> stream = builder.stream("input-topic", Consumed.with(Serdes.String(), genericRecordSerde));
 *   EvalBuilder<String> evalBuilder = new EvalBuilder<>(builder, stream, Serdes.String(), genericRecordSerde, "output-topic-base");
 *   evalBuilder.with("fieldName", (key, value) -> { ... });
 *   TopologyBuilder<String, GenericRecord>; topologyBuilder = evalBuilder.build();
 * }
 * </pre>
 *
 * <br>
 * <b>Pseudo DSL</b>
 * <pre>
 *   from
 *     {SOURCE[K:GenericRecord]}
 *
 *   <b>eval</b>
 *     ({FIELD} = {EXPRESSION})+
 *
 *   to
 *     {TARGET[K:GenericRecord]}
 * </pre>
 *
 * @param <K> the key type
 */
public class EvalBuilder<K> extends AbstractTopologyPartBuilder<K, GenericRecord> {

    private final List<Expression<K, GenericRecord>> expressions = new LinkedList<>();

    /**
     * Constructor for EvalBuilder class.
     *
     * @param streamsBuilder Kafka Streams builder to build topology
     * @param stream         input KStream for topology
     * @param keySerde       serde for the key
     * @param valueSerde     serde for the value
     * @param topicsBaseName base name for the topics
     */
    EvalBuilder(
            StreamsBuilder streamsBuilder,
            KStream<K, GenericRecord> stream,
            Serde<K> keySerde,
            Serde<GenericRecord> valueSerde,
            String topicsBaseName) {
        super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
    }

    /**
     * Add an expression to update a field in the GenericRecord value.
     *
     * @param fieldName     the name of the field to update
     * @param valueFunction a function that takes in the key and value, and returns the new value for the field
     * @return the EvalBuilder object for method chaining
     */
    public EvalBuilder<K> with(String fieldName, BiFunction<K, GenericRecord, Object> valueFunction) {
        Objects.requireNonNull(fieldName, "fieldName");
        Objects.requireNonNull(valueFunction, "valueFunction");

        this.expressions.add(new Expression<>(fieldName, valueFunction));

        return this;
    }

    /**
     * Build the topology and return a TopologyBuilder object
     *
     * @return the TopologyBuilder object representing the built topology
     */
    public TopologyBuilder<K, GenericRecord> build() {
        return createTopologyBuilder(
                this.stream
                        .map(
                                (key, value) -> {
                                    this.expressions.forEach(expression -> expression.update(key, value));
                                    return new KeyValue<>(key, value);
                                }));
    }

}
