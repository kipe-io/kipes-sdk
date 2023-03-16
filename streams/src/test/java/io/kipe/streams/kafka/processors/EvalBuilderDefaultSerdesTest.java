package io.kipe.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kipe.streams.kafka.serdes.GenericRecordSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.TopologyTestContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Test class for the {@link EvalBuilder}
 */
class EvalBuilderDefaultSerdesTest extends AbstractGenericRecordProcessorTopologyDefaultSerdesTest {

	public EvalBuilderDefaultSerdesTest() {
		super(getTopologySpecificProps());
	}

	private static Map<String, String> getTopologySpecificProps() {
		Map<String, String> props = new HashMap<>();
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericRecordSerdes.class.getName());
		return props;
	}

	/**
	 * This method adds the eval processor to the topology builder, with specific evaluation functions set for the keys
	 * outputA, outputB, and input. The evaluation functions take the key and value of the input record, and return a
	 * new value for the specified key. Placeholder for the actual builder implementation.
	 */
	@Override
	protected KipesBuilder<String, GenericRecord> addGenericRecordProcessor(
			KipesBuilder<String, GenericRecord> builder,
			TopologyTestContext topologyTestContext)
	{
		return builder
				.eval()
				.with("outputA", (key,value) -> value.get("input"))
				.with("outputB", (key,value) -> value.get("outputA"))
				.with("input", (key,value) -> "changed")
				.build();
	}

	// ------------------------------------------------------------------------
	// tests
	// ------------------------------------------------------------------------

	/**
	 * This test sends a test input to the topology, and asserts that the outputs of the {@link EvalBuilder} processor have the expected values.
	 * <p>
	 * The test is validating if the evaluator functions are working as expected for the keys outputA, outputB and input.
	 */
	@Test
	void test_eval_evals() {
		// given a GenericMessage
		send("input", "Hello World");

		// then there was a GenericMessage received with outputs set
		assertEquals(1, this.targetTopic.getQueueSize());

		GenericRecord r = this.targetTopic.readValue();
		assertEquals("Hello World", r.get("outputA"));
		assertEquals("Hello World", r.get("outputB"));
		assertEquals("changed", r.get("input"));

	}
}
