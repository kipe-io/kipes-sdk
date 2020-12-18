package de.tradingpulse.streams.kafka.factories;

public abstract class AbstractProcessorFactory extends AbstractStreamFactory {
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {};
	}
	
	@Override
	protected void doPostConstruct() throws Exception {
		initProcessors();
	}

	protected abstract void initProcessors() throws Exception;

}
