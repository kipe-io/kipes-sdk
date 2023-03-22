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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * TopicManager is a singleton class that provides methods to manage topics in a Kafka cluster.
 * It uses Apache Kafka AdminClient to communicate with the Kafka cluster.
 *
 */
@Singleton
public class TopicManager {
	
	static final Logger LOG = LoggerFactory.getLogger(TopicManager.class);
	static final RecordsToDelete DELETE_ALL_RECORDS = RecordsToDelete.beforeOffset(-1);

	// when manually created for tests the AdminClient will be null
	@Inject
	private AdminClient adminClient;
	
	public void ensureTopics(NewTopic... newTopics) throws InterruptedException, ExecutionException {
		ensureTopics(new HashSet<>(Arrays.asList(newTopics)));
	}

	/**
	 * This method creates the provided topics in the Kafka cluster.
	 * It first checks if the topics are already available in the cluster, if not then it creates them.
	 * It uses the AdminClient to create the topics.
	 *
	 * @param newTopics - Set of NewTopic objects that need to be created.
	 * @throws InterruptedException - when the thread is interrupted while waiting for the AdminClient response.
	 * @throws ExecutionException   - when the AdminClient fails to create the topics.
	 */
	public void ensureTopics(Set<NewTopic> newTopics) throws InterruptedException, ExecutionException {
		if(adminClient == null) {
			newTopics.forEach(topic -> LOG.warn("adminClient not injected, skipping creation of topic '{}'", topic));
			return;
		}
		
		Set<NewTopic> topicsToBeCreated = new HashSet<>();
		Set<String>	availableTopics = adminClient.listTopics().names().get();
		
		newTopics.forEach(newTopic -> {
			String topicName = newTopic.name();
			
			if (availableTopics.contains(topicName)) {
				LOG.info("topic '{}' is available", topicName);
				return;
			}
			
			topicsToBeCreated.add(newTopic);
		});
		
		adminClient.createTopics(topicsToBeCreated).all().get();
		
		topicsToBeCreated.forEach(createdTopic -> LOG.info("topic '{}' created", createdTopic.name()));
	}

	/**
	 * This method deletes the provided topics from the Kafka cluster.
	 * It uses the AdminClient to delete the topics.
	 *
	 * @param topics - Collection of topic names that need to be deleted.
	 * @throws InterruptedException - when the thread is interrupted while waiting for the AdminClient response.
	 * @throws ExecutionException   - when the AdminClient fails to delete the topics.
	 */
	public void deleteTopics(Collection<String> topics) throws InterruptedException, ExecutionException {
		if(adminClient == null) {
			topics.forEach(topic -> LOG.warn("adminClient not injected, skipping deletion of topic '{}'", topic));
			return;
		}
		adminClient.deleteTopics(topics).all().get();
	}

	/**
	 * Clears the provided topics from the Kafka cluster.
	 * It uses the AdminClient to clear the topics.
	 *
	 * @param topics - Collection of topic names that need to be deleted.
	 * @throws InterruptedException - when the thread is interrupted while waiting for the AdminClient response.
	 * @throws ExecutionException   - when the AdminClient fails to clear the topics.
	 */
	public void clearTopics(Collection<String> topics) throws InterruptedException, ExecutionException {
		if(adminClient == null) {
			topics.forEach(topic -> LOG.warn("adminClient not injected, skipping cleaning of topic '{}'", topic));
			return;
		}
		Collection<TopicDescription> topicDescriptions = adminClient.describeTopics(topics)
				.all().get()
				.values();
		
		Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
		
		topicDescriptions.forEach(topicDescription -> {
			String topicName = topicDescription.name();
			
			topicDescription.partitions().forEach(topicPartitionInfo -> 
				recordsToDelete.put(
						new TopicPartition(topicName, topicPartitionInfo.partition()), 
						DELETE_ALL_RECORDS)
			);
		});
		
		adminClient.deleteRecords(recordsToDelete).all().get();
		topicDescriptions.forEach(topicDescription -> LOG.info("topic '{}' removed all messages", topicDescription.name()));
	}
}
