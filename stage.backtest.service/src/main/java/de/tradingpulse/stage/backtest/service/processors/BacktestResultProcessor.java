package de.tradingpulse.stage.backtest.service.processors;

import javax.inject.Inject;
import javax.inject.Singleton;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.recordtypes.SignalExecutionRecord;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
public class BacktestResultProcessor extends AbstractProcessorFactory {
	
	@Inject
	private BacktestStreamsFacade backtestStreamsFacade;
	
	@Inject
	private ConfiguredStreamBuilder streamBuilder;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() throws Exception {
		// --------------------------------------------------------------------
		// from
		//		signal_excution_daily
		// transaction
		//		startswith signalRecord.signalType.type = ENTRY
		//  	endswith signalRecord.signalType.type = EXIT
		//  	on key.symbol
		//  	as TransactionRecord<SignalExecutionRecord>
		// map
		//		delta = records[-1].ohlcvRecord.close - record[0].ohlcvRecord.open
		//		as BacktestResultRecord
		// to
		//		backtestresult_daily	
		// --------------------------------------------------------------------
		
		TopologyBuilder
		.init(streamBuilder)
		.from(
				backtestStreamsFacade.getSignalExecutionDailyStream(), 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(SignalExecutionRecord.class))
		.<SignalExecutionRecord>transaction()
			.groupBy(groupKeyFunction, groupKeySerde)
			.startsWith(startsWithPredicate)
			.endsWith(endsWithPredicate)
			.collect();
	}
}
