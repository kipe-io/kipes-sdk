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

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import io.kipe.streams.kafka.serdes.TestRecordSerdes;
import io.kipe.streams.recordtypes.JoinRecord;
import io.kipe.streams.recordtypes.TestRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import io.kipe.common.utils.TimeUtils;
import io.kipe.streams.test.kafka.AbstractTopologyTest;
import io.kipe.streams.test.kafka.TopologyTestContext;

/**
 * Test class for the {@link JoinBuilder}. Tests the functionality of the Streams join operator by creating input topics
 * for left and right records and an output topic for joined records. Tests various join scenarios, including successful
 * and unsuccessful cases based on record timestamps.
 */
class JoinBuilderDefaultSerdesTest extends AbstractTopologyTest {

	private static final String LEFT_TOPIC = "leftTopic";
	private static final String RIGHT_TOPIC = "rightTopic";
	private static final String JOIN_TOPIC = "joinTopic";
	
	private static final int WINDOW_SIZE_AFTER = 7;
	
	private static final String KEY = "key";
	
	private TestInputTopic<String, TestRecord> leftTopic;
	private TestInputTopic<String, TestRecord> rightTopic;
	private TestOutputTopic<String, JoinRecord> joinTopic;

	public JoinBuilderDefaultSerdesTest() {
		super(getTopologySpecificProps());
	}

	private static Map<String, String> getTopologySpecificProps() {
		Map<String, String> props = new HashMap<>();
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TestRecordSerdes.class.getName());
		return props;
	}

	/**
	 * Initializes the topology for the test.
	 *
	 * @param topologyTestContext the {@link TopologyTestContext} instance for the test.
	 */
	@Override
	protected void initTopology(TopologyTestContext topologyTestContext) {
		KipesBuilder<?,?> builder = KipesBuilder.init(topologyTestContext.getStreamsBuilder());
		
		// setup time adjusted left stream
		KStream<String, TestRecord> leftStream = builder
				.<String,TestRecord>from(
					topologyTestContext.createKStream(LEFT_TOPIC))
				.adjustRecordTimestamps(
						(key, value) ->
							value.timestamp)
				.getStream();
		
		// setup time adjusted right stream
		KStream<String, TestRecord> rightStream = builder
				.<String,TestRecord>from(
					topologyTestContext.createKStream(RIGHT_TOPIC))

				.adjustRecordTimestamps(
						(key, value) ->
							value.timestamp)
				.getStream();
				
		// setup join and push result
		builder
		.withTopicsBaseName(JOIN_TOPIC)
		.from(leftStream)

		.<TestRecord, JoinRecord>join(rightStream)
			.withRetentionPeriod(Duration.ofDays(365))
			.withWindowSizeAfter(Duration.ofDays(WINDOW_SIZE_AFTER))
			.as(JoinRecord::from)

		.to(JOIN_TOPIC);
	}

	/**
	 * Initializes the input and output topics for the tests.
	 *
	 * @param topologyTestContext the context in which the test topics are created.
	 */
	@Override
	protected void initTestTopics(TopologyTestContext topologyTestContext) {
		this.leftTopic = topologyTestContext.createTestInputTopic(
				LEFT_TOPIC, 
				String.class, 
				TestRecord.class);
		
		this.rightTopic = topologyTestContext.createTestInputTopic(
				RIGHT_TOPIC, 
				String.class, 
				TestRecord.class);
		
		
		this.joinTopic = topologyTestContext.createTestOutputTopic(
				JOIN_TOPIC, 
				String.class, 
				JoinRecord.class);		
	}

	// ------------------------------------------------------------------------
	// tests - one record each
	// ------------------------------------------------------------------------

	/**
	 * Test to check if two records with the same timestamp will yield one join.
	 */
	@Test
	void test_same_timestamps__yields_one_join() {
		// given two messages at the same timestamp
		long timestamp = TimeUtils.getTimestampDaysBeforeNow(7);
		sendLeftTestRecord(timestamp);
		sendRightTestRecord(timestamp);
		
		// then we get one joined record
		assertEquals(1, this.joinTopic.getQueueSize());
	}

	/**
	 * Test to check if two records with a timestamp difference of WINDOW_SIZE_AFTER days will yield one join.
	 */
	@Test
	void test_window_size_days_after__yields_one_join() {
		// given two messages with a day difference
		long timestamp = TimeUtils.getTimestampDaysBeforeNow(7);
		sendLeftTestRecord(timestamp);
		sendRightTestRecord(timestamp+WINDOW_SIZE_AFTER*ONE_DAY);
		
		// then we get one joined record
		assertEquals(1, this.joinTopic.getQueueSize());
	}

	/**
	 * Test to check if two records with a timestamp difference of WINDOW_SIZE_AFTER + 1 days will yield no join.
	 */
	@Test
	void test_one_day_after_window_size__yields_no_join() {
		// given two messages with eight days difference
		long timestamp = TimeUtils.getTimestampDaysBeforeNow(7);
		sendLeftTestRecord(timestamp);
		sendRightTestRecord(timestamp+(WINDOW_SIZE_AFTER+1)*ONE_DAY);
		
		// then we get no joined record
		assertEquals(0, this.joinTopic.getQueueSize());
	}
	
	// ------------------------------------------------------------------------
	// tests - one record left, two right
	// ------------------------------------------------------------------------

	/**
	 * Test method for testing timestamps within window. It tests that two messages at the same timestamp will yield two
	 * join records.
	 */
	@Test
	void test_timestamps_within_window__yields_two_joins() {
		// given two messages at the same timestamp
		long timestamp = TimeUtils.getTimestampDaysBeforeNow(7);
		sendLeftTestRecord(timestamp);
		sendRightTestRecord(timestamp);
		sendRightTestRecord(timestamp + ONE_DAY);
		
		// then we get joined records
		assertEquals(2, this.joinTopic.getQueueSize());
	}
	
	// ------------------------------------------------------------------------
	// tests - two left, two right
	// ------------------------------------------------------------------------

	/**
	 * Test method for testing timestamps within window. It tests that four messages, two left and two right,
	 * will yield four join records.
	 */
	@Test
	void test_timestamps_within_window__yields_four_joins() {
		// given two messages at the same timestamp
		long timestamp = TimeUtils.getTimestampDaysBeforeNow(7);
		sendLeftTestRecord(timestamp);
		sendLeftTestRecord(timestamp + ONE_DAY);

		sendRightTestRecord(timestamp + ONE_DAY);
		sendRightTestRecord(timestamp + 2*ONE_DAY);
		
		// then we get joined records
		assertEquals(4, this.joinTopic.getQueueSize());
	}
	
	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------

	/**
	 * Sends a test record to the left topic.
	 *
	 * @param timestamp the timestamp of the record.
	 */
	private void sendLeftTestRecord(long timestamp) {
		sendTestRecord(this.leftTopic, timestamp);
	}

	/**
	 * Sends a test record to the right topic.
	 *
	 * @param timestamp the timestamp of the record.
	 */
	private void sendRightTestRecord(long timestamp) {
		sendTestRecord(this.rightTopic, timestamp);
	}

	/**
	 * Sends a test record to the specified topic.
	 *
	 * @param topic     the topic to which the record will be sent.
	 * @param timestamp the timestamp of the record.
	 */
	private void sendTestRecord(TestInputTopic<String, TestRecord> topic, long timestamp) {
		TestRecord r = new TestRecord(timestamp, KEY);
		topic.pipeInput(r.key, r, timestamp);
	}

}
