/*
 * Kipes SDK for Kafka - The High-Level Event Processing SDK.
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
