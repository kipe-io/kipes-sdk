package de.tradingpulse.stage.backtest.service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tradingpulse.stage.backtest.service.processors.BacktestResultProcessor;
import de.tradingpulse.stage.backtest.service.processors.SignalExecutionProcessor;
import io.micronaut.context.annotation.Requires;
import io.micronaut.runtime.Micronaut;

@Singleton
@Requires(
		beans = {
				SignalExecutionProcessor.class,
				BacktestResultProcessor.class
		})
public class Application {

	private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(final String[] args) {
        Micronaut.run(Application.class, args);
    }
	
	@PostConstruct
	void postConstruct() {
		LOG.info("initialized");
	}

	@PreDestroy
	void preDestroy() {
		LOG.info("shutting down");
	}
}
