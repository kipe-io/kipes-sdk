package de.tradingpulse.stage.backtest.service.processors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractTopologyTest {
	
	/** one day in milliseconds */
	protected static final long ONE_DAY = 86400000L;
	
	protected static final String STRATEGY_KEY = "strategyKey";
	protected static final String SYMBOL = "symbol";
	
	
	// @BeforeEach initializes the following members
	private TopologyTestContext topologyTestContext;
	
	@BeforeEach
    void beforeEachInitTopologyAndTopics() {
		this.topologyTestContext = TopologyTestContext.create();
		
		initTopology(this.topologyTestContext);
		
		this.topologyTestContext.initTopologyTestDriver();
		
		initTestTopics(this.topologyTestContext);
	}
	
	protected abstract void initTopology(TopologyTestContext topologyTestContext);

	protected abstract void initTestTopics(TopologyTestContext topologyTestContext);
	
	@AfterEach
	void afterEachCloseContext() {
		this.topologyTestContext.close();
	}

}
