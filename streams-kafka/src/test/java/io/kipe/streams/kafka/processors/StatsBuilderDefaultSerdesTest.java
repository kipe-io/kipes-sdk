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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import io.kipe.streams.kafka.processors.expressions.stats.Count;
import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.kafka.serdes.GenericRecordSerdes;
import io.kipe.streams.test.kafka.TopologyTestContext;

import java.util.HashMap;
import java.util.Map;

/**
 * This class test the functionality of {@link StatsBuilder} by counting the number of records grouped by 'group' field.
 */
class StatsBuilderDefaultSerdesTest extends AbstractGenericRecordProcessorTopologyDefaultSerdesTest {

	public StatsBuilderDefaultSerdesTest() {
		super(getTopologySpecificProps());
	}

	private static Map<String, String> getTopologySpecificProps() {
		Map<String, String> props = new HashMap<>();
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericRecordSerdes.class.getName());
		return props;
	}

	/**
	 * This method is used to add the stats processor to the topology builder It uses the Count.count() method to count
	 * the number of records in each group It groups the records by 'group' field and returns the topology builder.
	 *
	 * @param builder             KipesBuilder<String, GenericRecord>
	 * @param topologyTestContext TopologyTestContext
	 * @return KipesBuilder<String, GenericRecord>
	 */
	@Override
	protected KipesBuilder<String, GenericRecord> addGenericRecordProcessor(
			KipesBuilder<String, GenericRecord> builder, 
			TopologyTestContext topologyTestContext) 
	{
		return builder.stats()
				.with(Count.count()).as("myCount")
				.groupBy("group")
				.build();
	}

	/**
	 * This is the test method that asserts the functionality of the stats processor.
	 */
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
