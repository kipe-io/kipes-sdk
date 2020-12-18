package de.tradingpulse.streams.kafka.factories;

public class TopicNamesFactory {
	
	/**
	 * Returns the canonical topic name for processor store by appending 
	 * '-processor-store' to the given tbaseTopicName
	 */
	public static String getProcessorStoreTopicName(String baseTopicName) {
		return baseTopicName+"-processor-store";
	}

	private TopicNamesFactory() {}
}
