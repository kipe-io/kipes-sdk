package io.kipe.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import io.kipe.streams.kafka.processors.TopologyBuilder;
import io.kipe.streams.kafka.processors.expressions.stats.Count;
import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.TopologyTestContext;

/**
 * This class test the functionality of {@link StatsBuilder} by counting the number of records grouped by 'group' field.
 */
class StatsBuilderTest extends AbstractGenericRecordProcessorTopologyTest {

	/**
	 * This method is used to add the stats processor to the topology builder
	 * It uses the Count.count() method to count the number of records in each group
	 * It groups the records by 'group' field and returns the topology builder.
	 *
	 * @param builder             TopologyBuilder<String, GenericRecord>
	 * @param topologyTestContext TopologyTestContext
	 * @return TopologyBuilder<String, GenericRecord>
	 */
	@Override
	protected TopologyBuilder<String, GenericRecord> addGenericRecordProcessor(
			TopologyBuilder<String, GenericRecord> builder, 
			TopologyTestContext topologyTestContext) 
	{
		return builder.stats()
				.with(Count.count()).as("myCount")
				.groupBy("group")
				.build(topologyTestContext.getJsonSerdeRegistry().getSerde(String.class));
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
