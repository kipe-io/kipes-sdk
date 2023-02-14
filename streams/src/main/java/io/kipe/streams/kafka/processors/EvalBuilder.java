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
 * A builder that updates fields in a {@link GenericRecord} value based on expressions provided to the
 * {@link EvalBuilder#with(String, BiFunction)} method. Clients do not instantiate this class directly but use
 * {@link KipesBuilder#eval()}.
 * <p>
 *
 * <b>Example:</b>
 * <pre>{@code StreamsBuilder builder = new StreamsBuilder();
 * KStream<String, GenericRecord> stream = builder.stream("input-topic");
 * EvalBuilder<String> evalBuilder =
 *         new EvalBuilder<>(
 *                 builder,
 *                 stream,
 *                 Serdes.String(),
 *                 genericRecordSerde,
 *                 "output-topic-base"
 *         );
 *
 * KipesBuilder<String, GenericRecord> kipesBuilder = evalBuilder
 *         .with("fieldName", (key, value) -> "new-value")
 *         .build();
 * }</pre>
 * <p>
 * In this example, an instance of EvalBuilder is created using the StreamsBuilder and KStream objects, along with the
 * serdes for the key and value and the base name for the topics. Then, the with method is called to add an expression
 * to update the "fieldName" field with the value "new-value". Finally, the build method is called to build the topology
 * and return a KipesBuilder object.
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
     * @param fieldName     the name of the field to update.
     * @param valueFunction a function that takes in the key and value, and returns the new value for the field.
     * @return the EvalBuilder object for method chaining.
     */
    public EvalBuilder<K> with(String fieldName, BiFunction<K, GenericRecord, Object> valueFunction) {
        Objects.requireNonNull(fieldName, "fieldName");
        Objects.requireNonNull(valueFunction, "valueFunction");

        this.expressions.add(new Expression<>(fieldName, valueFunction));

        return this;
    }

    /**
     * Build the topology and return a KipesBuilder object.
     *
     * @return the KipesBuilder object representing the built topology.
     */
    public KipesBuilder<K, GenericRecord> build() {
        return createKipesBuilder(
                this.stream
                        .map(
                                (key, value) -> {
                                    this.expressions.forEach(expression -> expression.update(key, value));
                                    return new KeyValue<>(key, value);
                                }));
    }

}
