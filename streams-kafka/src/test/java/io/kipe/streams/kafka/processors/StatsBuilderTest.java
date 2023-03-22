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

import io.kipe.streams.kafka.processors.expressions.stats.Count;
import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.TopologyTestContext;

import java.util.Map;
class StatsBuilderTest extends AbstractGenericRecordProcessorTopologyTest {
	public StatsBuilderTest() {
		super(Map.of());
	}
	@Override
	protected KipesBuilder<String, GenericRecord> addGenericRecordProcessor(
			KipesBuilder<String, GenericRecord> builder, 
			TopologyTestContext topologyTestContext) 
	{
		return builder.stats()
				.with(Count.count()).as("myCount")
				.groupBy("group")
				.build(topologyTestContext.getJsonSerdeRegistry().getSerde(String.class));
	}
	@Test
	void test() {
		// given three records
		send("group", "A");
		send("group", "A");
		send("group", "B");
		
		// then we get two results
		assertEquals(3, this.targetTopic.getQueueSize());
		
		GenericRecord r = this.targetTopic.readValue();
		assertEquals("A", r.getString("group"));
		assertEquals(1, r.getNumber("myCount").intValue());
		
		r = this.targetTopic.readValue();
		assertEquals("A", r.getString("group"));
		assertEquals(2, r.getNumber("myCount").intValue());
		
		r = this.targetTopic.readValue();
		assertEquals("B", r.getString("group"));
		assertEquals(1, r.getNumber("myCount").intValue());
	}

}
