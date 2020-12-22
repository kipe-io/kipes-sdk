package de.tradingpulse.stage.backtest.service.processors;

import javax.inject.Inject;
import javax.inject.Singleton;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.recordtypes.SignalExecutionRecord;
import de.tradingpulse.stage.backtest.recordtypes.SignalType;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import de.tradingpulse.streams.recordtypes.TransactionRecord;
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
	@SuppressWarnings("unchecked")
	protected void initProcessors() throws Exception {
		// --------------------------------------------------------------------
		// from
		//		signal_excution_daily
		// transaction
		//		startswith signalRecord.signalType.type = ENTRY
		//  	endswith signalRecord.signalType.type = EXIT
		//  	on key.symbol and signalRecord.strategyKey
		//  	as TransactionRecord<SignalExecutionRecord, String>
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
		
		.<SignalExecutionRecord, String> transaction()
			.groupBy(
					(key, value) ->
						value.getKey().getSymbol() + "-" + value.getSignalRecord().getStrategyKey(), 
					jsonSerdeRegistry.getSerde(String.class))
			.startsWith(
					(key, value) ->
						value.getSignalRecord().getSignalType().is(SignalType.Type.ENTRY))
			.endsWith(
					(key, value) ->
						value.getSignalRecord().getSignalType().is(SignalType.Type.EXIT))
			.as(
					jsonSerdeRegistry.getSerde(
							(Class<TransactionRecord<SignalExecutionRecord, String>>)
							(Class<?>) TransactionRecord.class));
		
	}
}
