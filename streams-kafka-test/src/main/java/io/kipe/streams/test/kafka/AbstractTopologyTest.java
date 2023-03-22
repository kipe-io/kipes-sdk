/*
 * Kipes SDK Test Extensions for Kafka - The High-Level Event Processing SDK.
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
package io.kipe.streams.test.kafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Map;

/**
 * Abstract class for testing Kafka topologies.
 * <p>It provides common methods and variables for testing Kafka topologies using JUnit5 and the Kafka Streams Test Utils library.
 * <p>It defines a {@link TopologyTestContext} for holding the test context, including the topology and test driver.
 * <p>It also defines abstract methods {@link AbstractTopologyTest#initTopology(TopologyTestContext)} and {@link AbstractTopologyTest#initTestTopics(TopologyTestContext)} to be overridden in subclasses to initialize the topology and test topics respectively.
 * <p>It also contains lifecycle methods {@link AbstractTopologyTest#beforeEachInitTopologyAndTopics()} and {@link AbstractTopologyTest#afterEachCloseContext()} to be run before and after each test method respectively.
 */
public abstract class AbstractTopologyTest {
	
	public AbstractTopologyTest(Map<String, String> topologySpecificProps) {
		this.topologySpecificProps = topologySpecificProps;
	}
	
	/** one day in milliseconds */
	protected static final long ONE_DAY = 86400000L;
	
	protected static final String STRATEGY_KEY = "strategyKey";
	protected static final String SYMBOL = "symbol";
	
	
	// @BeforeEach initializes the following members
	private TopologyTestContext topologyTestContext;

	private final Map<String, String> topologySpecificProps;

	/**
	 * Initializes topology and test topics by calling {@link AbstractTopologyTest#initTopology(TopologyTestContext)}
	 * and {@link AbstractTopologyTest#initTestTopics(TopologyTestContext)}  respectively and initializing the topologyTestDriver.
	 */
	@BeforeEach
    void beforeEachInitTopologyAndTopics() {
		this.topologyTestContext = TopologyTestContext.create(topologySpecificProps);
		
		initTopology(this.topologyTestContext);
		
		this.topologyTestContext.initTopologyTestDriver();
		
		initTestTopics(this.topologyTestContext);
	}

	/**
	 * Override this method in subclasses to initialize topology.
	 *
	 * @param topologyTestContext TopologyTestContext instance to hold test context.
	 */
	protected abstract void initTopology(TopologyTestContext topologyTestContext);

	/**
	 * Override this method in subclasses to initialize test topics.
	 *
	 * @param topologyTestContext TopologyTestContext instance to hold test context.
	 */
	protected abstract void initTestTopics(TopologyTestContext topologyTestContext);

	/**
	 * Closes the TopologyTestContext instance.
	 */
	@AfterEach
	void afterEachCloseContext() {
		this.topologyTestContext.close();
	}

}
