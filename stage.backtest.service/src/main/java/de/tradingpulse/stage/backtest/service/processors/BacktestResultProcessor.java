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
		// Note: to export the results to TSV (you need to exit with CTRL-C)
		// kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic stg-backtest-backtestresult_daily --from-beginning | jq -r '[.key.symbol,.entryTimestamp,.key.timestamp,.strategyKey,.tradingDirection,.longRangeTrends.ema,.longRangeTrends.macdHistogram,.longRangeTrends.macdLinesSlope,.longRangeTrends.macdValue,.longRangeTrends.sstocSlope,.longRangeTrends.sstocValue,.shortRangeTrends.ema,.shortRangeTrends.macdHistogram,.shortRangeTrends.macdLinesSlope,.shortRangeTrends.macdValue,.shortRangeTrends.sstocSlope,.shortRangeTrends.sstocValue,.entry,.low,.high,.exit] | @tsv' - > ~/Desktop/signal_execs.tsv
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
						
						// sometimes there are only close values available.
						SignalExecutionRecord entryRecord = value.getRecord(0);
						double entry = entryRecord.getOhlcvRecord().getOpen() == 0.0? 
								entryRecord.getOhlcvRecord().getClose()
								: entryRecord.getOhlcvRecord().getOpen();
						SignalExecutionRecord exitRecord = value.getRecord(-1);
						double exit = exitRecord.getOhlcvRecord().getOpen() == 0.0? 
								exitRecord.getOhlcvRecord().getClose()
								: exitRecord.getOhlcvRecord().getOpen();
						double high = Math.max(entry, exit);
						double low = Math.min(entry, exit);

						for(SignalExecutionRecord record : value.getRecords()) {
							OHLCVRecord ohlcv = record.getOhlcvRecord();
							high = Math.max(high, ohlcv.getHigh());
							// sometimes there are only close values available.
							low = Math.min(low, ohlcv.getLow() == 0.0? ohlcv.getClose() : ohlcv.getLow());
						}
						
						return BacktestResultRecord.builder()
							.key(exitRecord.getKey().deepClone())
							.timeRange(exitRecord.getTimeRange())
							.strategyKey(entryRecord.getSignalRecord().getStrategyKey())
							.tradingDirection(entryRecord.getSignalRecord().getSignalType().getTradingDirection())
							.entryTimestamp(entryRecord.getTimeRangeTimestamp())
							.entry(entry)
							.high(high)
							.low(low)
							.exit(exit)
							.shortRangeTrends(entryRecord.getSignalRecord().getShortRangeTrends())
							.longRangeTrends(entryRecord.getSignalRecord().getLongRangeTrends())
							.build(); 
					})
			.asValueType(
					jsonSerdeRegistry.getSerde(BacktestResultRecord.class))
		.to(
				BacktestStreamsFacade.TOPIC_BACKTESTRESULT_DAILY);
		
	}
}
