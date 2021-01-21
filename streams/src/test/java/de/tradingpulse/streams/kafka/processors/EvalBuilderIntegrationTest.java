package de.tradingpulse.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Test;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;
import de.tradingpulse.streams.test.kafka.AbstractTopologyTest;
import de.tradingpulse.streams.test.kafka.TopologyTestContext;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;

class EvalBuilderIntegrationTest extends AbstractTopologyTest {
	
	private static final String SOURCE = "source";
	private static final String TARGET = "target";

	private TestInputTopic<String, GenericRecord> sourceTopic;
	private TestOutputTopic<String, GenericRecord> targetTopic;

	@Override
	protected void initTopology(TopologyTestContext topologyTestContext) {
		JsonSerdeRegistry serdes = topologyTestContext.getJsonSerdeRegistry();

		TopologyBuilder.init(topologyTestContext.getStreamsBuilder())
		.from( 
				topologyTestContext.createKStream(
						SOURCE, 
						String.class, 
						GenericRecord.class),
				serdes.getSerde(String.class),
				serdes.getSerde(GenericRecord.class))
		
		.withTopicsBaseName(SOURCE)
		
		.eval()
			.with("outputA", (key,value) -> value.get("input"))
			.with("outputB", (key,value) -> value.get("outputA"))
			.with("input", (key,value) -> "changed")
			.build()
		
		.to(TARGET);
	}

	@Override
	protected void initTestTopics(TopologyTestContext topologyTestContext) {
		this.sourceTopic = topologyTestContext.createTestInputTopic(
				SOURCE, 
				String.class, 
				GenericRecord.class);
		
		
		this.targetTopic = topologyTestContext.createTestOutputTopic(
				TARGET, 
				String.class, 
				GenericRecord.class);		
	}

	// ------------------------------------------------------------------------
	// tests
	// ------------------------------------------------------------------------

	@Test
	void test_eval_evals() {
		// given a GenericMessage 
		send("Hello World");
		
		// then there was a GenericMessage received with outputs set
		assertEquals(1, this.targetTopic.getQueueSize());
		
		GenericRecord r = this.targetTopic.readValue();
		assertEquals("Hello World", r.get("outputA"));
		assertEquals("Hello World", r.get("outputB"));
		assertEquals("changed", r.get("input"));
		
	}
	
	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------

	private void send(String inputValue) {
		this.sourceTopic.pipeInput(GenericRecord.create().with("input", inputValue));
	}
}
