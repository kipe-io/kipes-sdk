package de.tradingpulse.stage.sourcedata.service.processors;

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
public class SourceDataProcessorsFacade {

	private static final Logger LOG = LoggerFactory.getLogger(SourceDataProcessorsFacade.class);
	
	@Inject
	private OHLCVDailyProcessor dailyProcessor;

	@Inject
	private OHLCVWeeklyProcessor weeklyIncrementalProcessor;
	
	@PostConstruct
	void postConstruct() {
		LOG.info("initialized");
	}

	@PreDestroy
	void preDestroy() {
		LOG.info("shutting down");
	}
}
