package de.tradingpulse.streams.kafka.factories;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.kafka.clients.admin.NewTopic;

import io.micronaut.context.annotation.Value;

/**
 * Provides base functionality around streams.
 */
public abstract class AbstractStreamFactory {

	@Inject
	private TopicManager topicManager;
	
	@Value("${kafka.replication.factor}")
	private short replicationFactor;
	
	// ------------------------------------------------------------------------
	// init
	// ------------------------------------------------------------------------
	
	@PostConstruct
	void postConstruct() throws Exception {
		initTopics();
		doPostConstruct();
	}
	
	/**
	 * Default blank implementation. Overwrite if you need to do some initialization work.
	 */
	protected void doPostConstruct() throws Exception {
		// empty for overwriting purposes
	}
	
	/**
	 * Initializes all topics which names will be given by {@link #getTopicNames()}.
	 * 
	 * The method will create the topics if necessary.
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	protected void initTopics() throws InterruptedException, ExecutionException {
		ensureTopics(getTopicNames());
	}
	
	protected void ensureTopics(String...topicNames) throws InterruptedException, ExecutionException {
		Set<NewTopic> newTopics = Arrays.asList(topicNames).stream()
				// TODO externalize numPartitions 
				.map(topicName -> new NewTopic(topicName, 1, replicationFactor))
				.collect(Collectors.toSet());
		
		topicManager.ensureTopics(newTopics);
	}
	
	/**
	 * Returns the topic names for all the topics this factory takes care,
	 * notably for those it needs to create.
	 * 
	 * See {@link #getTopicNamesForDeletion()} to overwrite those which need
	 * to get deleted (in case you use that feature) 
	 */
	protected abstract String[] getTopicNames();

	/**
	 * Returns the raw topic names for all topics which need to get deleted.
	 * Defaults to {@link #getTopicNames()}. You can overwrite this method
	 * in order to add changelog topics in case you need to. 
	 */
	protected Set<String> getTopicNamesForDeletion() {
		return new HashSet<>(Arrays.asList(getTopicNames()));
	}
	
	public void deleteAllTopics() throws InterruptedException, ExecutionException {
		Set<String> topicNames = getTopicNamesForDeletion();
				
		topicManager.deleteTopics(topicNames);
	}
	
	// ------------------------------------------------------------------------
	// get/set
	// ------------------------------------------------------------------------
	
	public TopicManager getTopicManager() {
		return this.topicManager;
	}
}
