package de.tradingpulse.streams.kafka.factories;

public abstract class AbstractProcessorFactory extends AbstractStreamFactory {
	
	protected static String getProcessorStoreTopicName(String baseTopicName) {
		return baseTopicName+"-processor-store";
	}
	
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
