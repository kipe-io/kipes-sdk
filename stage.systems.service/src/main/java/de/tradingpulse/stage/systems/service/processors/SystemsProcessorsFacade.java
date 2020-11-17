package de.tradingpulse.stage.systems.service.processors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micronaut.context.annotation.Context;

@Singleton
@Context
@SuppressWarnings("unused")
public class SystemsProcessorsFacade {

	private static final Logger LOG = LoggerFactory.getLogger(SystemsProcessorsFacade.class);
	
	@Inject
	private ImpulseStreamProcessor impulseIncrementalStreamProcessor;

	@PostConstruct
	void postConstruct() {
		LOG.info("initialized");
	}

	@PreDestroy
	void preDestroy() {
		LOG.info("shutting down");
	}
}
