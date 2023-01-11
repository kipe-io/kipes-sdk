package io.kipe.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import io.kipe.streams.kafka.processors.TopologyBuilder;
import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.TopologyTestContext;

class EvalBuilderTest extends AbstractGenericRecordProcessorTopologyTest {

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
