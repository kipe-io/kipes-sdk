package io.kipe.streams.kafka.factories;

/**
 * Abstract base class for creating specific processor factories.
 */
public abstract class AbstractProcessorFactory extends AbstractStreamFactory {

	/**
	 * Returns an empty array of topic names.
	 *
	 * @return empty array of topic names.
	 */
	@Override
	protected String[] getTopicNames() {
		return new String[] {};
	}

	/**
	 * Invokes the initProcessors() method after the object is constructed.
	 *
	 * @throws Exception If an error occurs during processor initialization.
	 */
	@Override
	protected void doPostConstruct() throws Exception {
		initProcessors();
	}

	/**
	 * Abstract method for initializing processors in subclasses.
	 *
	 * @throws Exception If an error occurs during processor initialization.
	 */
	protected abstract void initProcessors() throws Exception;

}
