package de.tradingpulse.streams.test.kafka;

import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

public class TopologyTestContext {

	private static final Properties CONFIG = new Properties();
	static {
		CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
	}

	private static final JsonSerdeRegistry JSONSERDEREGISTRY = MockedJsonSerdeRegistry.create();
	
	public static TopologyTestContext create() {
		return new TopologyTestContext(
				new ConfiguredStreamBuilder(CONFIG));
	}
	
	private final ConfiguredStreamBuilder streamBuilder;	
	private TopologyTestDriver driver = null;	
	
	private TopologyTestContext(
			ConfiguredStreamBuilder streamBuilder)
	{
		this.streamBuilder = streamBuilder;
	}

	// ------------------------------------------------------------------------
	// state before initTopologyTestDriver
	// ------------------------------------------------------------------------

	public ConfiguredStreamBuilder getStreamsBuilder() {
		return this.streamBuilder;
	}
	
	public JsonSerdeRegistry getJsonSerdeRegistry() {
		return JSONSERDEREGISTRY;
	}
	
	public <K,V> KStream<K,V> createKStream(String topic, Class<K> keyClass, Class<V> valueClass) {
		return this.streamBuilder
				.stream(topic, Consumed.with(
						JSONSERDEREGISTRY.getSerde(keyClass),
						JSONSERDEREGISTRY.getSerde(valueClass))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
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
	
	public <K,V> TestInputTopic<K,V> createTestInputTopic(String topic, Class<K> keyType, Class<V> valueType) {
		Objects.requireNonNull(this.driver, "TopologyTestDriver must be initialized before by calling 'initTopologyTestDriver()'");
		return this.driver.createInputTopic(
				topic, 
				JSONSERDEREGISTRY.getSerializer(keyType), 
				JSONSERDEREGISTRY.getSerializer(valueType));
	}
	
	public <K,V> TestOutputTopic<K, V> createTestOutputTopic(String topic, Class<K> keyType, Class<V> valueType) {
		Objects.requireNonNull(this.driver, "TopologyTestDriver must be initialized before by calling 'initTopologyTestDriver()'");
		return this.driver.createOutputTopic(
				topic, 
				JSONSERDEREGISTRY.getDeserializer(keyType), 
				JSONSERDEREGISTRY.getDeserializer(valueType));
	}
	
	public void close() {
		Objects.requireNonNull(this.driver, "TopologyTestDriver must be initialized before by calling 'initTopologyTestDriver()'");
		this.driver.close();
		this.driver = null;
	}
	
}
