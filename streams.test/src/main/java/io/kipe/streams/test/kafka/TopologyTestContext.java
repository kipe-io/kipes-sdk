package io.kipe.streams.test.kafka;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

/**
 * TopologyTestContext is a utility class for setting up and testing Apache Kafka Streams topologies.
 * It provides a convenient way to create a {@link ConfiguredStreamBuilder} for building a topology,
 * and a {@link TopologyTestDriver} for testing the topology.
 *
 * <p>To use TopologyTestContext, create an instance using the static method {@link #create(Map)}
 * and use {@link #getStreamsBuilder()} to obtain a {@link ConfiguredStreamBuilder} to build the topology.
 * <p>Then use {@link #initTopologyTestDriver()} to obtain a {@link TopologyTestDriver} to test the topology.
 * <p>Finally, use {@link #createTestInputTopic(String, Class, Class)} and {@link #createTestOutputTopic(String, Class, Class)}
 * to create input and output topics, respectively.
 *
 * <p>The {@link JsonSerdeRegistry} provided by TopologyTestContext can be used to obtain serializers and deserializers for keys and values
 * in the input and output topics.
 *
 * <p>To close the TopologyTestContext, use {@link #close()}.
 */
public class TopologyTestContext {

	private static final Properties CONFIG = new Properties();
	static {
		CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
	}

	private static final JsonSerdeRegistry JSONSERDEREGISTRY = MockedJsonSerdeRegistry.create();

	/**
	 * Creates a new instance of TopologyTestContext.
	 * <p>
	 * This method accepts topology properties that are specific to sub-builder tests, such as default serdes.
	 *
	 * @param topologySpecificProps Topology properties passed into the test context.
	 * @return a new TopologyTestContext instance.
	 */
	public static TopologyTestContext create(Map<String, String> topologySpecificProps) {
		CONFIG.putAll(topologySpecificProps);
		return new TopologyTestContext(
				new ConfiguredStreamBuilder(CONFIG));
	}
	
	private final ConfiguredStreamBuilder streamBuilder;	
	private TopologyTestDriver driver = null;

    /**
     * Creates a new instance of {@code TopologyTestContext}.
     *
     * @param streamBuilder a configured stream builder
     */
	private TopologyTestContext(
			ConfiguredStreamBuilder streamBuilder)
	{
		this.streamBuilder = streamBuilder;
	}

	// ------------------------------------------------------------------------
	// state before initTopologyTestDriver
	// ------------------------------------------------------------------------

    /**
     * Returns the {@link ConfiguredStreamBuilder} instance associated with this {@code TopologyTestContext}.
     *
     * @return the {@link ConfiguredStreamBuilder} instance.
     */
	public ConfiguredStreamBuilder getStreamsBuilder() {
		return this.streamBuilder;
	}

    /**
     * Returns the {@link JsonSerdeRegistry} instance associated with this {@code TopologyTestContext}.
     *
     * @return the {@link JsonSerdeRegistry} instance.
     */
	public JsonSerdeRegistry getJsonSerdeRegistry() {
		return JSONSERDEREGISTRY;
	}

	/**
	 * Creates a new {@link KStream} instance with the specified topic, key class and value class.
	 * <p>
	 * It uses json serdes for the key and value class.
	 *
	 * @param <K>        the type of key
	 * @param <V>        the type of value
	 * @param topic      the topic name
	 * @param keyClass   the key class
	 * @param valueClass the value class
	 * @return a new {@link KStream} instance
	 */
	public <K,V> KStream<K,V> createKStream(String topic, Class<K> keyClass, Class<V> valueClass) {
		return this.streamBuilder
				.stream(topic, Consumed.with(
						JSONSERDEREGISTRY.getSerde(keyClass),
						JSONSERDEREGISTRY.getSerde(valueClass))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	/**
	 * Creates a new {@link KStream} instance with the specified topic.
	 * <p>
	 * It uses default serdes for the key and value class.
	 *
	 * @param <K>   the type of key
	 * @param <V>   the type of value
	 * @param topic the topic name
	 * @return a new {@link KStream} instance
	 */
	public <K,V> KStream<K,V> createKStream(String topic) {
		return this.streamBuilder
				.stream(topic);
	}

	// ------------------------------------------------------------------------
	// initTopologyTestDriver
	// ------------------------------------------------------------------------
	
	/**
	 * Once the topology has been set up with by using the
	 * {@link #getStreamsBuilder()} you can acquire a {@link TopologyTestDriver}
	 * here.
	 */
	public TopologyTestDriver initTopologyTestDriver() {
		this.driver = new TopologyTestDriver(this.streamBuilder.build(), CONFIG);
		return driver;
	}
	
	// ------------------------------------------------------------------------
	// state after initTopologyTestDriver
	// ------------------------------------------------------------------------

    /**
     * Creates a {@link TestInputTopic} for the given topic, key type, and value type.
     * The TopologyTestDriver must be initialized by calling 'initTopologyTestDriver()' before this method is called.
     *
     * @param topic     The name of the topic to create the TestInputTopic for.
     * @param keyType   The type of the key to be serialized.
     * @param valueType The type of the value to be serialized.
     * @param <K>       The type of the key.
     * @param <V>       The type of the value.
     * @return The created TestInputTopic.
     * @throws NullPointerException If the TopologyTestDriver has not been initialized.
     */
	public <K,V> TestInputTopic<K,V> createTestInputTopic(String topic, Class<K> keyType, Class<V> valueType) {
		Objects.requireNonNull(this.driver, "TopologyTestDriver must be initialized before by calling 'initTopologyTestDriver()'");
		return this.driver.createInputTopic(
				topic,
				JSONSERDEREGISTRY.getSerializer(keyType),
				JSONSERDEREGISTRY.getSerializer(valueType));
	}

    /**
     * Creates a {@link TestOutputTopic} for the given topic, key type, and value type.
     * The TopologyTestDriver must be initialized by calling 'initTopologyTestDriver()' before this method is called.
     *
     * @param topic     The name of the topic to create the TestOutputTopic for.
     * @param keyType   The type of the key to be deserialized.
     * @param valueType The type of the value to be deserialized.
     * @param <K>       The type of the key.
     * @param <V>       The type of the value.
     * @return The created TestOutputTopic.
     * @throws NullPointerException If the TopologyTestDriver has not been initialized.
     */
	public <K,V> TestOutputTopic<K, V> createTestOutputTopic(String topic, Class<K> keyType, Class<V> valueType) {
		Objects.requireNonNull(this.driver, "TopologyTestDriver must be initialized before by calling 'initTopologyTestDriver()'");
		return this.driver.createOutputTopic(
				topic,
				JSONSERDEREGISTRY.getDeserializer(keyType),
				JSONSERDEREGISTRY.getDeserializer(valueType));
	}

    /**

     Closes the TopologyTestDriver.
     The TopologyTestDriver must be initialized by calling 'initTopologyTestDriver()' before this method is called.
     @throws NullPointerException If the TopologyTestDriver has not been initialized.
     */
	public void close() {
		Objects.requireNonNull(this.driver, "TopologyTestDriver must be initialized before by calling 'initTopologyTestDriver()'");
		this.driver.close();
		this.driver = null;
	}
	
}
