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
	
	public void deleteTopics(Collection<String> topics) throws InterruptedException, ExecutionException {
		if(adminClient == null) {
			topics.forEach(topic -> LOG.warn("adminClient not injected, skipping deletion of topic '{}'", topic));
			return;
		}
		adminClient.deleteTopics(topics).all().get();
	}
	
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
