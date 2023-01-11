package io.kipe.streams.kafka.factories;

public class TopicNamesFactory {
	
	/**
	 * Returns the canonical topic name for a processor store by appending 
	 * '-processor-store' to the given tbaseTopicName
	 */
	public static String getProcessorStoreTopicName(String baseTopicName) {
		return baseTopicName+"-processor-store";
	}
	
	/**
	 * Returns the canonical topic name for a grouping repartition topic
	 * '-grouped' to the given tbaseTopicName
	 */
	public static String getGroupedTopicName(String baseTopicName) {
		return baseTopicName+"-grouped";
	}

	private TopicNamesFactory() {}
}
