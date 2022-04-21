package de.tradingpulse.streams.kafka.processors;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;

import de.tradingpulse.streams.recordtypes.GenericRecord;
import de.tradingpulse.streams.test.kafka.AbstractTopologyTest;
import de.tradingpulse.streams.test.kafka.TopologyTestContext;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;

public abstract class AbstractGenericRecordProcessorTopologyTest extends AbstractTopologyTest {
	
	protected static final String SOURCE = "source";
	protected static final String TARGET = "target";

	protected TestInputTopic<String, GenericRecord> sourceTopic;
	protected TestOutputTopic<String, GenericRecord> targetTopic;


	@Override
	protected void initTopology(TopologyTestContext topologyTestContext) {
		JsonSerdeRegistry serdes = topologyTestContext.getJsonSerdeRegistry();

		TopologyBuilder<String, GenericRecord> builder = TopologyBuilder.init(topologyTestContext.getStreamsBuilder())
		.from( 
				topologyTestContext.createKStream(
						SOURCE, 
						String.class, 
						GenericRecord.class),
				serdes.getSerde(String.class),
				serdes.getSerde(GenericRecord.class))
		
		.withTopicsBaseName(SOURCE);
		
		addGenericRecordProcessor(builder, topologyTestContext)
		.to(TARGET);
	}

	protected abstract TopologyBuilder<String, GenericRecord> addGenericRecordProcessor(
			TopologyBuilder<String, GenericRecord> builder, 
			TopologyTestContext topologyTestContext);
	
	@Override
	protected void initTestTopics(TopologyTestContext topologyTestContext) {
		this.sourceTopic = topologyTestContext.createTestInputTopic(
				SOURCE, 
				String.class, 
				GenericRecord.class);
		
		
		this.targetTopic = topologyTestContext.createTestOutputTopic(
				TARGET, 
				String.class, 
				GenericRecord.class);		
	}


	protected <V> void send(String field, V value) {
		this.sourceTopic.pipeInput(GenericRecord.create().with(field, value));
	}

}
