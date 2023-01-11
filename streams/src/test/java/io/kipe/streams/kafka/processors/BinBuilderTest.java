package io.kipe.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.of;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kipe.streams.kafka.processors.TopologyBuilder;
import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.TopologyTestContext;

class BinBuilderTest extends AbstractGenericRecordProcessorTopologyTest {

	@Override
	protected TopologyBuilder<String, GenericRecord> addGenericRecordProcessor(
			TopologyBuilder<String, GenericRecord> builder, 
			TopologyTestContext topologyTestContext) 
	{
		return builder
				.bin()
				.field("input")
				.span(0.1)
				.build(); // note that we aren't setting the "newField", so "input" will be overwritten
	}

	// ------------------------------------------------------------------------
	// tests
	// ------------------------------------------------------------------------

	@ParameterizedTest
	@MethodSource("createTestData")
	void test_bin_discretizes(double inputValue, double discretizedValue) {
		// given a GenericMessage 
		send("input", inputValue);
		
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

}
