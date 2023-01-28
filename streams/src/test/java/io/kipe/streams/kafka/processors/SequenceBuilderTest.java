package io.kipe.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Test;

import io.kipe.streams.kafka.processors.TopologyBuilder;
import io.kipe.streams.test.kafka.AbstractTopologyTest;
import io.kipe.streams.test.kafka.TopologyTestContext;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


/**
 * Test class for {@link SequenceBuilder}
 */
class SequenceBuilderTest extends AbstractTopologyTest {

	private static final String SOURCE = "source";
	private static final String TARGET = "target";

	private TestInputTopic<String, TestRecord> sourceTopic;
	private TestOutputTopic<String, TestRecord> targetTopic;

	/**
	 * Initialize the topology for testing.
	 * <p>
	 * Creates a Kafka Streams topology with a source topic, a sequence processor, and a target topic.
	 *
	 * @param topologyTestContext context for topology testing
	 */
	@Override
	protected void initTopology(TopologyTestContext topologyTestContext) {
		JsonSerdeRegistry serdes = topologyTestContext.getJsonSerdeRegistry();
		
		TopologyBuilder.init(topologyTestContext.getStreamsBuilder())
		.from( 
				topologyTestContext.createKStream(
						SOURCE, 
						String.class, 
						TestRecord.class),
				serdes.getSerde(String.class),
				serdes.getSerde(TestRecord.class))
		
		.withTopicsBaseName(SOURCE)
		
		.<String, TestRecord> sequence()
			.groupBy(
					(key, value) ->
						key, 
					serdes.getSerde(String.class))
			.size(3)
			.as(
					(key, records) -> {
						int sum = 0;
						long ts = 0;
						for(TestRecord record : records) {
							sum += record.getValue();
							ts = record.getTimestamp();
							record.count += 1; // changes the stored record!
						}
						return new TestRecord(ts, key, sum, records.get(0).count);
					},
					TestRecord.class,
					serdes.getSerde(TestRecord.class))
			
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
				TestRecord.class);
		
		
		this.targetTopic = topologyTestContext.createTestOutputTopic(
				TARGET, 
				String.class, 
				TestRecord.class);		
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
		TestRecord r = this.targetTopic.readValue();
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
		TestRecord r = this.targetTopic.readValue();
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
		this.sourceTopic.pipeInput(key, new TestRecord(timestamp, key, value, 0));
	}
	
	// ------------------------------------------------------------------------
	// records
	// ------------------------------------------------------------------------

	/**
	 * Data class for the test records.
	 */
	@AllArgsConstructor
	@NoArgsConstructor
	@Data
	public static class TestRecord {
		long timestamp;
		String key;
		Integer value;
		Integer count;
	}

}
