package de.tradingpulse.stage.backtest.service.processors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;

@Singleton
@Context
@SuppressWarnings("unused")
public class BacktestProcessorsFacade {

	private static final Logger LOG = LoggerFactory.getLogger(BacktestProcessorsFacade.class);
		
	@Inject
	private SignalExecutionProcessor signalExecutionProcessor;
	
	@Inject
	private BacktestResultProcessor backtestResultProcessor;
	
	@PostConstruct
	void postConstruct() {
		LOG.info("initialized");
	}

	@PreDestroy
	void preDestroy() {
		LOG.info("shutting down");
	}

}
