package de.tradingpulse.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.of;

import java.util.stream.Stream;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;
import de.tradingpulse.streams.test.kafka.AbstractTopologyTest;
import de.tradingpulse.streams.test.kafka.TopologyTestContext;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;

class BinBuilderIntegrationTest extends AbstractTopologyTest {
	
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
		
		.bin()
			.field("input")
			.span(0.1)
			.build() // note that we aren't setting the "newField", so "input" will be overwritten
		
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

	@ParameterizedTest
	@MethodSource("createTestData")
	void test_bin_discretizes(double inputValue, double discretizedValue) {
		// given a GenericMessage 
		send(inputValue);
		
		// then there was a GenericMessage
		assertEquals(1, this.targetTopic.getQueueSize());
		
		GenericRecord r = this.targetTopic.readValue();
		assertEquals(discretizedValue, r.get("input"));		
	}
	
	static Stream<Arguments> createTestData() {
		return Stream.of(
				of(-5.01, -5.0),
				of(-0.01,  0.0),
				of( 0.01,  0.0),
				of( 0.51,  0.5),
				of( 1.56,  1.6),
				of( 0.0 ,  0.0));
	}
	
	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------

	private void send(Double inputValue) {
		this.sourceTopic.pipeInput(GenericRecord.create().with("input", inputValue));
	}

}
