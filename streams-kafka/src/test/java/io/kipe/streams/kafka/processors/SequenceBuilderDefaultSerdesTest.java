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

import io.kipe.streams.kafka.serdes.TestRecordSequenceSerdes;
import io.kipe.streams.recordtypes.TestRecordSequence;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Test;

import io.kipe.streams.test.kafka.AbstractTopologyTest;
import io.kipe.streams.test.kafka.TopologyTestContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Test class for {@link SequenceBuilder}
 */
class SequenceBuilderDefaultSerdesTest extends AbstractTopologyTest {

	private static final String SOURCE = "source";
	private static final String TARGET = "target";

	private TestInputTopic<String, TestRecordSequence> sourceTopic;
	private TestOutputTopic<String, TestRecordSequence> targetTopic;

	public SequenceBuilderDefaultSerdesTest() {
		super(getTopologySpecificProps());
	}

	private static Map<String, String> getTopologySpecificProps() {
		Map<String, String> props = new HashMap<>();
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TestRecordSequenceSerdes.class.getName());
		return props;
	}

	/**
	 * Initialize the topology for testing.
	 * <p>
	 * Creates a Kafka Streams topology with a source topic, a sequence processor, and a target topic.
	 *
	 * @param topologyTestContext context for topology testing
	 */
	@Override
	protected void initTopology(TopologyTestContext topologyTestContext) {
		KipesBuilder.init(topologyTestContext.getStreamsBuilder())
		.<String, TestRecordSequence>from(
				topologyTestContext.createKStream(SOURCE))
		
		.withTopicsBaseName(SOURCE)
		
		.<String, TestRecordSequence> sequence()
			.groupBy(
					(key, value) ->
						key)
			.size(3)
			.as(
					(key, records) -> {
						int sum = 0;
						long ts = 0;
						for(TestRecordSequence record : records) {
							sum += record.getValue();
							ts = record.getTimestamp();
							record.count += 1; // changes the stored record!
						}
						return new TestRecordSequence(ts, key, sum, records.get(0).count);
					},
					TestRecordSequence.class)
			
		.to(TARGET);
		
	}

	/**
	 * Initialize the test topics for testing.
	 * <p>
	 * Creates test input and output topics for testing
	 *
	 * @param topologyTestContext context for topology testing
	 */
	@Override
	protected void initTestTopics(TopologyTestContext topologyTestContext) {
		this.sourceTopic = topologyTestContext.createTestInputTopic(
				SOURCE, 
				String.class,
				TestRecordSequence.class);
		
		
		this.targetTopic = topologyTestContext.createTestOutputTopic(
				TARGET, 
				String.class,
				TestRecordSequence.class);
	}

	// ------------------------------------------------------------------------
	// tests
	// ------------------------------------------------------------------------

	/**
	 * Test case for single group key
	 */
	@Test
	void test_single_group() {
		// when we send four records for one key
		send("key", 1, 10);
		send("key", 2, 20);
		send("key", 3, 30);
		send("key", 4, 40);
		
		// then we get two resulting records
		assertEquals(2, this.targetTopic.getQueueSize());
		
		// and the first record is #3 with the sum of 1..3
		TestRecordSequence r = this.targetTopic.readValue();
		assertEquals("key", r.key);
		assertEquals(30, r.timestamp);
		assertEquals(1+2+3, r.value);
		assertEquals(1, r.count);
		
		// and the second record is #4 with the sum of 2..4
		r = this.targetTopic.readValue();
		assertEquals(40, r.timestamp);
		assertEquals(2+3+4, r.value);
		assertEquals(2, r.count);
	}

	/**
	 * Test for handling two groups of records.
	 */
	@Test
	void test_two_groups() {
		// when we send two record groups 
		send("key_A",  1,  5);
		send("key_B", 10, 10);
		send("key_A",  2, 15);
		send("key_B", 20, 20);
		send("key_A",  3, 25);
		send("key_B", 30, 30);
		
		// then we get two resulting records
		assertEquals(2, this.targetTopic.getQueueSize());
		
		// and the first record is #A3 with the sum of 1..3
		TestRecordSequence r = this.targetTopic.readValue();
		assertEquals("key_A", r.key);
		assertEquals(25, r.timestamp);
		assertEquals(1+2+3, r.value);
		assertEquals(1, r.count);
		
		// and the second record is #B3 with the sum of 10..30
		r = this.targetTopic.readValue();
		assertEquals("key_B", r.key);
		assertEquals(30, r.timestamp);
		assertEquals(10+20+30, r.value);
		assertEquals(1, r.count);
	}
	
	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------

	/**
	 * Utility method for sending records to the source topic.
	 *
	 * @param key       the key of the record
	 * @param value     the value of the record
	 * @param timestamp the timestamp of the record
	 */
	private void send(String key, Integer value, long timestamp) {
		this.sourceTopic.pipeInput(key, new TestRecordSequence(timestamp, key, value, 0));
	}

}
