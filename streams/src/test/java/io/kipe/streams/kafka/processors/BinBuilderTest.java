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

/**
 * Test class for {@link TopologyBuilder#bin()} method.
 * Test the discretization of input values using the bin method.
 */
class BinBuilderTest extends AbstractGenericRecordProcessorTopologyTest {

	/**
	 * Overrides the addGenericRecordProcessor method to build the topology
	 * with bin method.
	 *
	 * @param builder             the topology builder to use.
	 * @param topologyTestContext the test context to use.
	 * @return the builder with the bin method added.
	 */
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

	/**
	 * Test the discretization of input values using the bin method.
	 *
	 * @param inputValue       the value to be discretized.
	 * @param discretizedValue the expected discretized value.
	 */
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

	/**
	 * Creates test data for the test_bin_discretizes method.
	 *
	 * @return the test data.
	 */
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
