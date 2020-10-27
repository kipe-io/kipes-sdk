package de.tradingpulse.stage.sourcedata.service.processors;

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
public class SourceDataProcessorsFacade {

	private static final Logger LOG = LoggerFactory.getLogger(SourceDataProcessorsFacade.class);
	
	@Inject
	private OHLCVDailyProcessor dailyProcessor;

	@Inject
	private OHLCVWeeklyIncrementalProcessor weeklyIncrementalProcessor;
	
	@PostConstruct
	void postConstruct() {
		LOG.info("initialized");
	}

	@PreDestroy
	void preDestroy() {
		LOG.info("shutting down");
	}
}
