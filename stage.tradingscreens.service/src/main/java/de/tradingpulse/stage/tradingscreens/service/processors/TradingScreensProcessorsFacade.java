package de.tradingpulse.stage.tradingscreens.service.processors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tradingpulse.stage.tradingscreens.streams.TrendsStream;
import io.micronaut.context.annotation.Context;

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
