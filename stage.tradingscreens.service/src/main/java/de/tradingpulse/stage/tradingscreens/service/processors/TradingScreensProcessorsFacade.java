package de.tradingpulse.stage.tradingscreens.service.processors;

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
public class TradingScreensProcessorsFacade {
	
	private static final Logger LOG = LoggerFactory.getLogger(TradingScreensProcessorsFacade.class);
	
	@Inject
	private TrendsProcessor trendsProcessor;
	
	@Inject
	private ImpulseTradingScreenProcessor impulseIncrementalStreamProcessor;

	@Inject
	private SignalsProcessor signalsProcessor;
	
	@PostConstruct
	void postConstruct() {
		LOG.info("initialized");
	}

	@PreDestroy
	void preDestroy() {
		LOG.info("shutting down");
	}

}
