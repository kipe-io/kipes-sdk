/*
 * Kipes SDK for Kafka - The High-Level Event Processing SDK.
 * Copyright © 2023 kipe.io
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

import org.junit.jupiter.api.Test;

import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.TopologyTestContext;

import java.util.Map;

/**
 * Test class for the {@link EvalBuilder}
 */
class EvalBuilderTest extends AbstractGenericRecordProcessorTopologyTest {

	public EvalBuilderTest() {
		super(Map.of());
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
