package de.tradingpulse.stage.backtest.service.processors;

import java.time.Duration;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.BacktestStageConstants;
import de.tradingpulse.stage.backtest.recordtypes.SignalExecutionRecord;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
public class SignalExecutionProcessor extends AbstractProcessorFactory {

	private static final String TOPIC_SIGNAL_DAILY_BY_SYMBOL =  BacktestStageConstants.STAGE_NAME + "-" + "signal_daily_by_symbol";
	private static final String TOPIC_OHLCV_DAILY_BY_SYMBOL =  BacktestStageConstants.STAGE_NAME + "-" + "ohlcv_daily_by_symbol";

	@Inject
	private SourceDataStreamsFacade sourceDataStreamsFacade;
	
	@Inject
	private TradingScreensStreamsFacade tradingScreensStreamsFacade;
	
	@Inject
	private BacktestStreamsFacade backtestStreamsFacade;
	
	@Inject
	private ConfiguredStreamBuilder streamBuilder;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_SIGNAL_DAILY_BY_SYMBOL,
				TOPIC_OHLCV_DAILY_BY_SYMBOL
		};
	}

	@Override
	protected void initProcessors() {
		// --------------------------------------------------------------------
		// from 
		//   signal_daily
		// inner join
		//   ohlcv_daily 
		//   on signal_daily.key.symbol = ohlcv_daily.key.symbol
		//   window before 0 after 7 days retention 2 years 
		//   as SignalExecutionRecord
		// filter 
		//   # remove the first element of the stream as execution can only happen-after the signal
		//   signalRecord.timeRangeTimestamp < ohlcvRecord.timeRangeTimestamp
		// dedup
		//   # we assume correct order to select the first element of the stream
		//   on signalRecord.key
		// to
		//   signal_execution_daily
		// --------------------------------------------------------------------
		
		TopologyBuilder<?,?> topologyBuilder = TopologyBuilder.init(streamBuilder);
		
		// rekeying signal_daily to key.symbol
		KStream<String, SignalRecord> rekeyedSignalStream = topologyBuilder
				.withTopicsBaseName(tradingScreensStreamsFacade.getSignalDailyStreamName())
				.from(
						tradingScreensStreamsFacade.getSignalDailyStream(),
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class),
						jsonSerdeRegistry.getSerde(SignalRecord.class))
				
				.rekey(
						(key, value) -> 
							key.getSymbol(),
						jsonSerdeRegistry.getSerde(String.class))
				
				.through(
						TOPIC_SIGNAL_DAILY_BY_SYMBOL)
				.getStream();
		
		// rekeying ohlcv_daily to key.symbol
		KStream<String, OHLCVRecord> rekeyedOhlcvStream = topologyBuilder
				.withTopicsBaseName(sourceDataStreamsFacade.getOhlcvDailyStreamName())
				.from(
						sourceDataStreamsFacade.getOhlcvDailyStream(),
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class),
						jsonSerdeRegistry.getSerde(OHLCVRecord.class))
				
				.rekey(
						(key, value) -> 
							key.getSymbol(),
						jsonSerdeRegistry.getSerde(String.class))
				
				.through(
						TOPIC_OHLCV_DAILY_BY_SYMBOL)
				.getStream();
		
		// main topology
		topologyBuilder
		.withTopicsBaseName(backtestStreamsFacade.getSignalExecutionDailyStreamName())
		.from(
				rekeyedSignalStream,
				jsonSerdeRegistry.getSerde(String.class),
				jsonSerdeRegistry.getSerde(SignalRecord.class))
		
		.<OHLCVRecord, SignalExecutionRecord> join(
				rekeyedOhlcvStream, 
				jsonSerdeRegistry.getSerde(OHLCVRecord.class))
			
			.withWindowSizeAfter(Duration.ofDays(7))
			.withRetentionPeriod(Duration.ofMillis(this.retentionMs + 86400000L)) // we add a day to have today access to the full retention time (record create ts is start of day)
			.as(
				SignalExecutionRecord::from, 
				jsonSerdeRegistry.getSerde(SignalExecutionRecord.class))
			
		.filter(
				(key, record) -> 
					record.getSignalRecord().getTimeRangeTimestamp() < record.getOhlcvRecord().getTimeRangeTimestamp())
		
		.<SymbolTimestampKey,Void> dedup()
			.groupBy(
				(key, record) -> 
					record.getSignalRecord().getKey(),
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class))
			.emitFirst()
			
		.rekey(
				(key, value) -> 
					value.getKey(),
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class))
		.to(
				backtestStreamsFacade.getSignalExecutionDailyStreamName());
	}
}
