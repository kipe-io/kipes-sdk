package de.tradingpulse.stage.systems.service.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micronaut.context.annotation.Context;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@Context
@SuppressWarnings("unused")
public class SystemsProcessorsFacade {

	private static final Logger LOG = LoggerFactory.getLogger(SystemsProcessorsFacade.class);
	
	@Inject
	private ImpulseStreamProcessor impulseIncrementalStreamProcessor;

	@Inject
	private TrendEMAProcessor trendEMAProcessor;
	
	@Inject
	private TrendMACDProcessor trendMACDProcessor;
	
	@Inject
	private TrendSSTOCProcessor trendSSTOCProcessor;

	@Inject
	private TrendAggregatorProcessor trendAggregatorProcessor;

	@PostConstruct
	void postConstruct() {
		LOG.info("initialized");
	}

	@PreDestroy
	void preDestroy() {
		LOG.info("shutting down");
	}
}
