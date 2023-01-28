package io.kipe.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import io.kipe.common.utils.TimeUtils;
import io.kipe.streams.kafka.processors.TopologyBuilder;
import io.kipe.streams.test.kafka.AbstractTopologyTest;
import io.kipe.streams.test.kafka.TopologyTestContext;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * This class is responsible for testing the functionality of the Streams join operator. It creates input topics for left and right records,
 * and an output topic for joined records. The tests in this class cover various scenarios, including cases where the join is successful
 * and cases where it is not successful due to the timestamps of the records not falling within the specified window size.
 */
class JoinBuilderTest extends AbstractTopologyTest {

	private static final String LEFT_TOPIC = "leftTopic";
	private static final String RIGHT_TOPIC = "rightTopic";
	private static final String JOIN_TOPIC = "joinTopic";
	
	private static final int WINDOW_SIZE_AFTER = 7;
	
	private static final String KEY = "key";
	
	private TestInputTopic<String, TestRecord> leftTopic;
	private TestInputTopic<String, TestRecord> rightTopic;
	private TestOutputTopic<String, JoinRecord> joinTopic;

	/**
	 * Initializes the topology for the test.
	 *
	 * @param topologyTestContext the {@link TopologyTestContext} instance for the test.
	 */
	@Override
	protected void initTopology(TopologyTestContext topologyTestContext) {
		JsonSerdeRegistry serdes = topologyTestContext.getJsonSerdeRegistry();
		
		TopologyBuilder<?,?> builder = TopologyBuilder.init(topologyTestContext.getStreamsBuilder());
		
		// setup time adjusted left stream
		KStream<String, TestRecord> leftStream = builder
				.from(
					topologyTestContext.createKStream(
							LEFT_TOPIC, 
							String.class, 
							TestRecord.class),
					serdes.getSerde(String.class), 
					serdes.getSerde(TestRecord.class))
				
				.adjustRecordTimestamps(
						(key, value) ->
							value.timestamp)
				.getStream();
		
		// setup time adjusted right stream
		KStream<String, TestRecord> rightStream = builder 
				.from(
					topologyTestContext.createKStream(
							RIGHT_TOPIC, 
							String.class, 
							TestRecord.class),
					serdes.getSerde(String.class), 
					serdes.getSerde(TestRecord.class))
				
				.adjustRecordTimestamps(
						(key, value) ->
							value.timestamp)
				.getStream();
				
		// setup join and push result 
		builder
		.withTopicsBaseName(JOIN_TOPIC)
		.from(
				leftStream, 
				serdes.getSerde(String.class), 
				serdes.getSerde(TestRecord.class))
		
		.<TestRecord, JoinRecord>join(
				rightStream, 
				serdes.getSerde(TestRecord.class))
			.withRetentionPeriod(Duration.ofDays(365))
			.withWindowSizeAfter(Duration.ofDays(WINDOW_SIZE_AFTER))
			.as(
					JoinRecord::from, 
					serdes.getSerde(JoinRecord.class))
		
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
	 * Test method for testing timestamps within window. It tests that two messages at the same timestamp
	 * will yield two join records.
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
	
	// ------------------------------------------------------------------------
	// records 
	// ------------------------------------------------------------------------

	/**
	 * TestRecord class that contains a timestamp and a key.
	 */
	@AllArgsConstructor
	@NoArgsConstructor
	@Data
	public static class TestRecord {
		long timestamp;
		String key;
	}

	/**
	 * JoinRecord class that contains a timestamp, a key, and two TestRecord objects.
	 * <p>
	 * It also contains a method 'from' to create a new JoinRecord from two TestRecord objects.
	 */
	@AllArgsConstructor
	@NoArgsConstructor
	@Data
	public static class JoinRecord {
		static JoinRecord from(TestRecord left, TestRecord right) {
			return new JoinRecord(
					Math.max(left.timestamp, right.timestamp), 
					left.key, 
					left, 
					right);
		}
		
		long timestamp;
		String key;
		
		TestRecord left;
		TestRecord right;
	}
}
