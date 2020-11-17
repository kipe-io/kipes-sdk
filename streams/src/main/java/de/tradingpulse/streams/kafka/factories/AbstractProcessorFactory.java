package de.tradingpulse.streams.kafka.factories;

public abstract class AbstractProcessorFactory extends AbstractStreamFactory {
	
	/**
	 * Returns the canonical topic name for processor store by appending 
	 * '-processor-store' to the given tbaseTopicName
	 */
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
