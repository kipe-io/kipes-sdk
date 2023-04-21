package io.kipe.streams.kafka.processors;

import static io.kipe.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;

import io.micronaut.core.serialize.exceptions.SerializationException;

/**
 * Builds sequences of records and applies a function to the sequences. Each
 * record starts a new sequence of the configured size. Clients do not instantiate
 * this class directly but use {@link KipesBuilder#sequence()}.
 *
 * <p><b>Usage</b></p>
 * To use the `SequenceBuilder` class, create an instance of the class, passing in a `StreamsBuilder`, `KStream`, `Serde` for key, `Serde` for value, and a topics base name.
 * Then, configure the `SequenceBuilder` with a group by function and size of sequences.
 * Finally, apply an aggregate function to the complete sequence of records for each group key.
 *
 * <p><b>Example</b></p>
 *
 * <pre>
 * {@code
 * StreamsBuilder streamsBuilder = new StreamsBuilder();
 * KStream<String, String> stream = streamsBuilder.stream("input-topic");
 * Serde<String> stringSerde = Serdes.String();
 *
 * SequenceBuilder<String, String, String, String> sequenceBuilder = new SequenceBuilder<>(
 *     streamsBuilder,
 *     stream,
 *     stringSerde,
 *     stringSerde,
 *     "topics-base-name"
 * );
 *
 * sequenceBuilder.groupBy((key, value) -> value, stringSerde)
 *     .size(5)
 *     .as((key, values) -> values.toString());
 *  }
 *  </pre>
 *
 * <p><b>Pseudo DSL</b></p>
 * <pre>
 *   from
 *     {SOURCE[key:value]}
 *
 *   <b>sequence</b>
 *     <b>groupBy</b>
 *       {FUNCTION[key,value]:groupKey}
 *     <b>size</b>
 *       {INTEGER:1}
 *     <b>as</b>
 *       {FUNCTION[value[]]:aggregate}
 *
 *   to
 *     {TARGET[key:newValue]}
 * </pre>
 *
 * @param <K>  The type of the key in the input stream
 * @param <V>  The type of the value in the input stream
 * @param <GK> The type of the group key
 * @param <VR> The type of the new value in the output stream
 */
// TODO: document potential record changing behavior of the aggregateFunction 
public class SequenceBuilder<K, V, GK, VR> extends AbstractTopologyPartBuilder<K, V> {

    private BiFunction<K, V, GK> groupKeyFunction;
    private Serde<GK> groupKeySerde;

    private int sequenceSize = 1;

    /**
     * Constructs a new SequenceBuilder.
     *
     * @param streamsBuilder The StreamsBuilder used to build the Kafka Streams topology
     * @param stream         The input KStream for this sequence builder
     * @param keySerde       The Serde for the key of the input stream
     * @param valueSerde     The Serde for the value of the input stream
     * @param topicsBaseName The base name for the topics used in this builder
     */
    SequenceBuilder(
            StreamsBuilder streamsBuilder,
            KStream<K, V> stream,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            String topicsBaseName) {
        super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
    }

    /**
     * Configures a GroupKeyFunction to group incoming records.
     *
     * @param groupKeyFunction the function to calculate the GroupKey
     * @param groupKeySerde    the serde for the GroupKey
     * @return this builder
     */
    public SequenceBuilder<K, V, GK, VR> groupBy(BiFunction<K, V, GK> groupKeyFunction, Serde<GK> groupKeySerde) {
        this.groupKeyFunction = groupKeyFunction;
        this.groupKeySerde = groupKeySerde;

        return this;
    }

    /**
     * Configures the size of the sequences. The given argument must be
     * greater than 0.
     * `
     *
     * @param size the size of the sequences.
     * @return this builder
     */
    public SequenceBuilder<K, V, GK, VR> size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be larger than 0");
        }

        this.sequenceSize = size;
        return this;
    }

    /**
     * Applies an aggregate function to the complete sequence of records for each group key. The aggregate function takes in the group key and a list of values and returns a new aggregate value.
     * Note that it is possible to alter the sequence records for later aggregations.
     *
     * @param aggregateFunction the function to apply to the complete sequence of records for each group key.
     * @param valueClass        the class of the input values in the sequence.
     * @param resultValueSerde  the serde for the aggregate value.
     * @return a new {@link KipesBuilder} with the aggregate value as the value type.
     */
    public KipesBuilder<K, VR> as(
            BiFunction<GK, List<V>, VR> aggregateFunction,
            Class<V> valueClass,
            Serde<VR> resultValueSerde) {
        Objects.requireNonNull(getTopicsBaseName(), "topicsBaseName");
        Objects.requireNonNull(this.groupKeyFunction, "groupKeyFunction");
        Objects.requireNonNull(this.groupKeySerde, "groupKeySerde");
        Objects.requireNonNull(resultValueSerde, "resultValueSerde");

        final String stateStoreName = getProcessorStoreTopicName(getTopicsBaseName() + "-sequence");

        StoreBuilder<KeyValueStore<GK, List<V>>> dedupStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                        this.groupKeySerde,
                        new SequencesSerde<>(valueClass));
        this.streamsBuilder.addStateStore(dedupStoreBuilder);


        return createKipesBuilder(
                this.stream
                        .transform(
                                () -> new SequenceTransformer<>(
                                        stateStoreName,
                                        this.groupKeyFunction,
                                        this.sequenceSize,
                                        aggregateFunction),
                                stateStoreName),
                this.keySerde,
                resultValueSerde);
    }


    // ------------------------------------------------------------------------
    // SequenceTransformer
    // ------------------------------------------------------------------------

    /**
     * SequenceTransformer is a transformer class that aggregates sequences of values into a single value using a BiFunction.
     * <p>
     * It takes in a state store name, a group key function, a sequence size, and an aggregate function.
     * <p>
     * The group key function is used to group input key-value pairs by a specific key, and the aggregate function is used to aggregate the values of each group into a single value.
     * <p>
     * The transformer keeps track of each group's values in the state store, and when the number of values for a group reaches the sequence size, it applies the aggregate function to the group's values and emits the result.
     *
     * @param <K>  the type of input keys.
     * @param <V>  the type of input values.
     * @param <VR> the type of output values.
     * @param <GK> the type of the group key.
     */
    static class SequenceTransformer<K, V, VR, GK>
            implements Transformer<K, V, KeyValue<K, VR>> {
        private final String stateStoreName;
        private final BiFunction<K, V, GK> groupKeyFunction;
        private final int sequenceSize;
        private final BiFunction<GK, List<V>, VR> aggregateFunction;

        KeyValueStore<GK, List<V>> stateStore;

        /**
         * Constructs a new SequenceTransformer.
         *
         * @param stateStoreName    the name of the state store to be used.
         * @param groupKeyFunction  a function that takes in a record key and value and returns a group key.
         * @param sequenceSize      the size of the sequence used to trigger the aggregate function.
         * @param aggregateFunction a function that takes in a group key and a list of records and returns an aggregate result.
         */
        SequenceTransformer(
                String stateStoreName,
                BiFunction<K, V, GK> groupKeyFunction,
                int sequenceSize,
                BiFunction<GK, List<V>, VR> aggregateFunction) {
            this.stateStoreName = stateStoreName;
            this.groupKeyFunction = groupKeyFunction;
            this.sequenceSize = sequenceSize;
            this.aggregateFunction = aggregateFunction;
        }

        /**
         * Initialize the transformer by getting the state store from the provided ProcessorContext.
         *
         * @param context the ProcessorContext to get the state store from.
         */
        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.stateStore = context.getStateStore(stateStoreName);
        }

        /**
         * The main method of the transformer, it takes in a record key and value, groups it by the group key, and applies the aggregate function if the sequence size has been reached.
         *
         * @param key   the key of the input record.
         * @param value the value of the input record.
         * @return a {@link KeyValue} object with the key of the input record and the aggregate result if the sequence size has been reached, or null if the sequence is not yet complete.
         */
        @Override
        public KeyValue<K, VR> transform(K key, V value) {
            final GK groupKey = this.groupKeyFunction.apply(key, value);
            List<V> groupSequence = this.stateStore.get(groupKey);

            if (groupSequence == null) {
                // we see that group for the very first time
                groupSequence = new LinkedList<>();

                // TODO: the group store grows indefinitly
                // outdated group keys aren't evicted, we might want to add
                // something like a ttl to evict incomplete sequences to free
                // up old keys
            }

            groupSequence.add(value);

            // aggregate the first sequence if it has all the records needed
            if (groupSequence.size() < this.sequenceSize) {
                this.stateStore.put(groupKey, groupSequence);
                return null;
            }

            KeyValue<K, VR> returnValue = new KeyValue<>(key, this.aggregateFunction.apply(groupKey, groupSequence));

            // we store now as the aggregateFunction may had altered the incoming records
            groupSequence.remove(0);
            this.stateStore.put(groupKey, groupSequence);

            return returnValue;
        }

        /**
         * Method is not used in this class.
         */
        @Override
        public void close() {
            // nothing to do
        }

    }

    /**
     * This class is a Serde (Serializer/Deserializer) for lists of a specific type T.
     * <p>
     * It uses the Jackson ObjectMapper to convert the lists to and from JSON.
     *
     * @param <T> the type of objects in the list.
     */
    static class SequencesSerde<T> implements Serializer<List<T>>, Deserializer<List<T>>, Serde<List<T>> {

        private final ObjectMapper mapper;
        private final CollectionType valueType;

        /**
         * Constructor for the class.
         *
         * @param type the class of the type of objects in the list.
         */
        public SequencesSerde(Class<T> type) {
            this.mapper = new ObjectMapper();


            this.valueType = mapper.getTypeFactory()
                    .constructCollectionType(List.class, type);
        }

        /**
         * Deserializes a byte array of JSON data into a list of objects of type T.
         *
         * @param topic the topic the data is from.
         * @param data  the byte array of JSON data.
         * @return the deserialized list of objects.
         */
        @Override
        public List<T> deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                return mapper.readValue(data, valueType);
            } catch (IOException e) {
                throw new SerializationException("Unable to deserialize data: " + data, e);
            }
        }

        /**
         * Serializes a list of objects of type T into a byte array of JSON data.
         *
         * @param topic the topic to send the data to.
         * @param data  the list of objects to serialize.
         * @return the byte array of JSON data.
         */
        @Override
        public byte[] serialize(String topic, List<T> data) {
            if (data == null) {
                return null;
            }

            try {
                return mapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new SerializationException("Unable to serialize data: " + data, e);
            }
        }

        /**
         * Method is not used in this class.
         *
         * @param configs configuration for the serde.
         * @param isKey   indicates if the serde is for the key or value.
         */
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // no-op
        }


        /**
         * Returns the serializer for the class.
         *
         * @return the serializer for the class.
         */
        @Override
        public Serializer<List<T>> serializer() {
            return this;
        }

        /**
         * Returns the deserializer for the class.
         *
         * @return the deserializer for the class.
         */
        @Override
        public Deserializer<List<T>> deserializer() {
            return this;
        }

        /**
         * Method is not used in this class.
         */
        @Override
        public void close() {
            // no-op
        }
    }


}
