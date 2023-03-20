package io.kipe.streams.kafka.processors;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;

import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.AbstractTopologyTest;
import io.kipe.streams.test.kafka.TopologyTestContext;

import java.util.Map;

/**
 * Abstract class for testing topologies that process {@link GenericRecord}s.
 * This class provides common functionality for creating test input and output topics,
 * and for initializing the topology under test.
 * Subclasses should provide a concrete implementation of the {@link #addGenericRecordProcessor(KipesBuilder, TopologyTestContext)} method
 * to add the specific processor being tested to the topology.
 */
public abstract class AbstractGenericRecordProcessorTopologyDefaultSerdesTest extends AbstractTopologyTest {
	
	protected static final String SOURCE = "source";
	protected static final String TARGET = "target";

	protected TestInputTopic<String, GenericRecord> sourceTopic;
	protected TestOutputTopic<String, GenericRecord> targetTopic;

	public AbstractGenericRecordProcessorTopologyDefaultSerdesTest(Map<String, String> topologySpecificProps) {
		super(topologySpecificProps);
	}

	/**
	 * Initialize the topology under test.
	 *
	 * @param topologyTestContext the context for the test.
	 */
	@Override
	protected void initTopology(TopologyTestContext topologyTestContext) {
		KipesBuilder<String, GenericRecord> builder = KipesBuilder.init(topologyTestContext.getStreamsBuilder())
		.<String,GenericRecord>from(topologyTestContext.createKStream(SOURCE))
		.withTopicsBaseName(SOURCE);
		
		addGenericRecordProcessor(builder, topologyTestContext)
		.to(TARGET);
	}

	/**
	 * Add the specific processor being tested to the topology.
	 *
	 * @param builder             the topology builder.
	 * @param topologyTestContext the context for the test.
	 * @return the topology builder.
	 */
	protected abstract KipesBuilder<String, GenericRecord> addGenericRecordProcessor(
			KipesBuilder<String, GenericRecord> builder,
			TopologyTestContext topologyTestContext);

	/**
	 * Initialize the test input and output topics.
	 *
	 * @param topologyTestContext the context for the test.
	 */
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

	/**
	 * Send a {@link GenericRecord} to the input topic.
	 *
	 * @param field the field to set on the {@link GenericRecord}.
	 * @param value the value to set on the {@link GenericRecord}.
	 */
	protected <V> void send(String field, V value) {
		this.sourceTopic.pipeInput(GenericRecord.create().with(field, value));
	}

}
