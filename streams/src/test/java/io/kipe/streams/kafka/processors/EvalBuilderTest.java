package io.kipe.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import io.kipe.streams.kafka.processors.TopologyBuilder;
import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.TopologyTestContext;

/**
 * This class tests the EvalBuilder class which is a part of the TopologyBuilder in the kipe.streams.kafka.processors package.
 * <p>
 * The EvalBuilder class is used to add a transformation step to the topology that performs evaluations on the key and value of the input record.
 * <p>
 * The test case creates a topology with a single EvalBuilder processor, and sends a test input to the topology.
 * <p>
 * The test case then asserts that the outputs of the EvalBuilder processor have the expected values.
 *
 * @see TopologyBuilder
 * @see EvalBuilder
 */
class EvalBuilderTest extends AbstractGenericRecordProcessorTopologyTest {

    /**
     * This method adds the eval processor to the topology builder, with specific evaluation functions set for the keys outputA, outputB, and input.
     * The evaluation functions take the key and value of the input record, and return a new value for the specified key.
     * Placeholder for the actual builder implementation.
     */
	@Override
	protected TopologyBuilder<String, GenericRecord> addGenericRecordProcessor(
			TopologyBuilder<String, GenericRecord> builder,
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
