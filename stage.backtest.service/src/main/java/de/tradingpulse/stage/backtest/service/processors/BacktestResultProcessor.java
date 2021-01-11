package de.tradingpulse.stage.backtest.service.processors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.recordtypes.BacktestResultRecord;
import de.tradingpulse.stage.backtest.recordtypes.SignalExecutionRecord;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import de.tradingpulse.streams.recordtypes.TransactionRecord;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
public class BacktestResultProcessor extends AbstractProcessorFactory {
	
	@Inject
	BacktestStreamsFacade backtestStreamsFacade;
	
	@Inject
	ConfiguredStreamBuilder streamBuilder;

	@Inject
	JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() throws Exception {
		createTopology(backtestStreamsFacade.getSignalExecutionDailyStream());
	}
	
	@SuppressWarnings("unchecked")
	void createTopology(
			KStream<SymbolTimestampKey, SignalExecutionRecord> signalExecutionDailyStream)
	{
		// --------------------------------------------------------------------
		// from
		//		signal_excution_daily
		// transaction
		//		startswith signalRecord.signalType.type = ENTRY
		//  	endswith signalRecord.signalType.type = EXIT
		//  	on key.symbol and signalRecord.strategyKey
		//  	as TransactionRecord<String, SignalExecutionRecord>
		// 
		// transform
		//		changeValue 
		//		as BacktestResultRecord
		// to
		//		backtestresult_daily	
		// --------------------------------------------------------------------
		
		TopologyBuilder
		.init(streamBuilder)
		.from(
				signalExecutionDailyStream, 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(SignalExecutionRecord.class))
		
		.withTopicsBaseName(BacktestStreamsFacade.TOPIC_SIGNAL_EXECUTION_DAILY)
		
		.<String, SignalExecutionRecord> transaction()
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
							(Class<TransactionRecord<String, SignalExecutionRecord>>)
							(Class<?>) TransactionRecord.class))
			
		.<SymbolTimestampKey, BacktestResultRecord> transform()
			.changeValue(
					(key, value) -> {
						
						OHLCVRecord entryRecord = value.getRecord(0).getOhlcvRecord();
						// sometimes there are only close values available.
						double entry = entryRecord.getOpen() == 0.0? entryRecord.getClose() : entryRecord.getOpen();
						double exit = value.getRecord(-1).getOhlcvRecord().getClose();
						double high = Math.max(entry, exit);
						double low = Math.min(entry, exit);

						for(SignalExecutionRecord record : value.getRecords()) {
							OHLCVRecord ohlcv = record.getOhlcvRecord();
							high = Math.max(high, ohlcv.getHigh());
							// sometimes there are only close values available.
							low = Math.min(low, ohlcv.getLow() == 0.0? ohlcv.getClose() : ohlcv.getLow());
						}
						
						return BacktestResultRecord.builder()
							.key(value.getKey().deepClone())
							.timeRange(value.getTimeRange())
							.strategyKey(value.getRecord(0).getSignalRecord().getStrategyKey())
							.tradingDirection(value.getRecord(0).getSignalRecord().getSignalType().getTradingDirection())
							.entryTimestamp(value.getRecord(0).getTimeRangeTimestamp())
							.entry(entry)
							.high(high)
							.low(low)
							.exit(exit)
							.build(); 
					})
			.asValueType(
					jsonSerdeRegistry.getSerde(BacktestResultRecord.class))
		.to(
				BacktestStreamsFacade.TOPIC_BACKTESTRESULT_DAILY);
		
	}
}
