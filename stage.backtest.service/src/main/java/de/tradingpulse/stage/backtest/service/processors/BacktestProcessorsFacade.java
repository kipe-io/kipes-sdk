package de.tradingpulse.stage.backtest.service.processors;

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
public class BacktestProcessorsFacade {

	private static final Logger LOG = LoggerFactory.getLogger(BacktestProcessorsFacade.class);
	
	@Inject
	private SignalExecutionProcessor signalExecutionProcessor;
	
	@Inject
	private BacktestResultProcessor backtestResultProcessor;
	
//	@Inject
//	private BacktestResultAnalyticsProcessor backtestResultAnalyticsProcessor;
	
	@PostConstruct
	void postConstruct() {
		LOG.info("initialized");
	}

	@PreDestroy
	void preDestroy() {
		LOG.info("shutting down");
	}

}
