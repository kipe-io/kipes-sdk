package de.tradingpulse.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;
import de.tradingpulse.streams.kafka.processors.expressions.stats.Count;
import de.tradingpulse.streams.test.kafka.TopologyTestContext;

class StatsBuilderTest extends AbstractGenericRecordProcessorTopologyTest {

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
