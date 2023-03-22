/*
 * Kipe Streams Kafka - Kipe Streams SDK
 * Copyright © 2023 Kipe.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.kipe.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.of;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import io.kipe.streams.kafka.serdes.GenericRecordSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.TopologyTestContext;

/**
 * Test class for {@link BinBuilder}. It tests the discretization of input values with the bin method.
 */
class BinBuilderDefaultSerdesTest extends AbstractGenericRecordProcessorTopologyDefaultSerdesTest {

	public BinBuilderDefaultSerdesTest() {
		super(getTopologySpecificProps());
	}

	private static Map<String, String> getTopologySpecificProps() {
		Map<String, String> props = new HashMap<>();
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericRecordSerdes.class.getName());
		return props;
	}

	/**
	 * Overrides the addGenericRecordProcessor method to build the topology with bin method.
	 *
	 * @param builder             the topology builder to use.
	 * @param topologyTestContext the test context to use.
	 * @return the builder with the bin method added.
	 */
	@Override
	protected KipesBuilder<String, GenericRecord> addGenericRecordProcessor(
			KipesBuilder<String, GenericRecord> builder,
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
