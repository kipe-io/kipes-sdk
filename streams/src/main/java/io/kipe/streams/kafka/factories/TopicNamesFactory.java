package io.kipe.streams.kafka.factories;

/**
 * Factory @class to generate canonical topic names for different types of topics.
 */
public class TopicNamesFactory {

	/**
	 * Returns the canonical topic name for a grouping repartition topic by
	 * appending '-grouped' to the given baseTopicName.
	 *
	 * @param baseTopicName the base topic name
	 * @return the canonical topic name for a grouping repartition topic
	 */
	public static String getProcessorStoreTopicName(String baseTopicName) {
		return baseTopicName+"-processor-store";
	}

	/**
	 * Returns the canonical topic name for a grouping repartition topic by
	 * appending '-grouped' to the given baseTopicName.
	 *
	 * @param baseTopicName the base topic name
	 * @return the canonical topic name for a grouping repartition topic
	 */
	public static String getGroupedTopicName(String baseTopicName) {
		return baseTopicName+"-grouped";
	}

	/**
	 * Private constructor to prevent instantiation of this factory class.
	 */
	private TopicNamesFactory() {}
}
