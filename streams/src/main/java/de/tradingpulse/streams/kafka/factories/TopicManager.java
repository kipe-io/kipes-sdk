package de.tradingpulse.streams.kafka.factories;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class TopicManager {
	
	static final Logger log = LoggerFactory.getLogger(TopicManager.class);
	static final RecordsToDelete DELETE_ALL_RECORDS = RecordsToDelete.beforeOffset(-1);

	@Inject
	private AdminClient adminClient;
	
	public void ensureTopics(NewTopic... newTopics) throws InterruptedException, ExecutionException {
		ensureTopics(new HashSet<>(Arrays.asList(newTopics)));
	}
	
	public void ensureTopics(Set<NewTopic> newTopics) throws InterruptedException, ExecutionException {
		Set<NewTopic> topicsToBeCreated = new HashSet<>();
		Set<String>	availableTopics = adminClient.listTopics().names().get();
		
		newTopics.forEach(newTopic -> {
			String topicName = newTopic.name();
			
			if (availableTopics.contains(topicName)) {
				log.info("topic '{}' is available", topicName);
				return;
			}
			
			topicsToBeCreated.add(newTopic);
		});
		
		adminClient.createTopics(topicsToBeCreated).all().get();
		
		topicsToBeCreated.forEach(createdTopic -> log.info("topic '{}' created", createdTopic.name()));
	}
	
	public void deleteTopics(Collection<String> topics) throws InterruptedException, ExecutionException {
		adminClient.deleteTopics(topics).all().get();
	}
	
	public void clearTopics(Collection<String> topics) throws InterruptedException, ExecutionException {
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
		topicDescriptions.forEach(topicDescription -> log.info("topic '{}' removed all messages", topicDescription.name()));
	}
}
