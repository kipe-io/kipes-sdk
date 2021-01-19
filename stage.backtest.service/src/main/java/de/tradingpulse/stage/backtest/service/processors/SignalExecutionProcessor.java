package de.tradingpulse.stage.backtest.service.processors;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.BacktestStageConstants;
import de.tradingpulse.stage.backtest.recordtypes.SignalExecutionRecord;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.Type;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import de.tradingpulse.streams.kafka.processors.TransactionBuilder.EmitType;
import de.tradingpulse.streams.recordtypes.TransactionRecord;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
public class SignalExecutionProcessor extends AbstractProcessorFactory {

	private static final String TOPIC_SIGNAL_DAILY_EXPANDED = BacktestStageConstants.STAGE_NAME + "-signal_daily_expanded";
	
	@Inject
	SourceDataStreamsFacade sourceDataStreamsFacade;
	
	@Inject
	TradingScreensStreamsFacade tradingScreensStreamsFacade;
	
	@Inject
	ConfiguredStreamBuilder streamBuilder;

	@Inject
	JsonSerdeRegistry jsonSerdeRegistry;

	/**
	 * Creates a list of SignalRecords starting with the first
	 * SignalRecord[ENTRY] from the given transaction, followed by {@code n} 
	 * SignalRecords[ONGOING], and ending with the second SignalRecord[EXIT]
	 * from the given transaction. <br>
	 * There will be as much {@code n} SignalRecords[ONGOING] as there are days
	 * between excluding the first and the second SignalRecord of the given
	 * transaction. 
	 */
	static List<KeyValue<SymbolTimestampKey, SignalRecord>> createContinuousDailySignalRecords(
			TransactionRecord<String, SignalRecord> transactionRecord) 
	{
		List<KeyValue<SymbolTimestampKey, SignalRecord>> keyValues = new LinkedList<>();

		SignalRecord entrySignal = transactionRecord.getRecord(0);
		SignalRecord exitSignal = transactionRecord.getRecord(1);
		long currentTimestampMS = entrySignal.getTimeRangeTimestamp();
		long exitTimestampMS = exitSignal.getTimeRangeTimestamp();
		

		// add entry SignalRecord
		keyValues.add(new KeyValue<>(entrySignal.getKey(), entrySignal));
		
		// add ongoing SignalRecords for each day after entry until before exit
		SignalType ongoingSignalType = entrySignal.getSignalType().as(Type.ONGOING);
		do {
			currentTimestampMS += 86400000;
			if(currentTimestampMS >= exitTimestampMS) {
				break;
			}
			SignalRecord currentOngoingSignal = entrySignal.deepClone();
			currentOngoingSignal.getKey().setTimestamp(currentTimestampMS);
			currentOngoingSignal.setSignalType(ongoingSignalType);
			keyValues.add(new KeyValue<>(currentOngoingSignal.getKey(), currentOngoingSignal));
		} while(true);
		
		// add exit SignalRecord
		keyValues.add(new KeyValue<>(exitSignal.getKey(), exitSignal));
		
		return keyValues;
		
	}
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_SIGNAL_DAILY_EXPANDED
		};
	}
	
	@Override
	protected void initProcessors() {
		createTopology(
				tradingScreensStreamsFacade.getSignalDailyStream(),
				sourceDataStreamsFacade.getOhlcvDailyStream());
	}
	
	@SuppressWarnings("unchecked")
	void createTopology(
			KStream<SymbolTimestampKey, SignalRecord> signalDailyStream,
			KStream<SymbolTimestampKey, OHLCVRecord> ohlcvDailyStream)
	{
		// --------------------------------------------------------------------
		// from
		//   signal_daily
		//
		// transaction
		//   startsWith signalType.type = ENTRY
		//   endsWith signalType.type = EXIT
		//   groupBy key.symbol, strategyKey
		//   emit START,END
		//   as TransactionRecord<String, SignalRecord>
		//
		// transform
		//   into {createContinuousDailySignalRecords}
		//   as {SymbolTimestampKey, SignalRecord}
		//
		// transform
		//   into key.timestamp += 1 day
		//   as SymbolTimestampKey
		//
		// inner join
		//   ohlcv_daily
		//   on signal_daily.key.symbol = ohlcv_daily.key.symbol
		//   window before 0 after 7 days retention 2 years 1 day
		//   as SignalExecutionRecord
		//
		// dedup
		//   # we assume correct order to select the first element of the stream
		//   on signalRecord
		//
		// to
		//   signal_execution_daily
		// --------------------------------------------------------------------
		
		KStream<String, OHLCVRecord> ohlcvRekeyed = TopologyBuilder.init(streamBuilder)
				.withTopicsBaseName(SourceDataStreamsFacade.TOPIC_OHLCV_DAILY)
				.from(
						ohlcvDailyStream, 
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class),
						jsonSerdeRegistry.getSerde(OHLCVRecord.class))
				// rekey
				.<String, SignalRecord>transform()
				.changeKey(
						(key, value) -> 
							key.getSymbol())
				.asKeyType(jsonSerdeRegistry.getSerde(String.class))
				.getStream();
				
		
		TopologyBuilder.init(streamBuilder)
		.withTopicsBaseName(TradingScreensStreamsFacade.TOPIC_SIGNAL_DAILY)
				
		// stream of SignalRecords per symbol and day
		.from(
				signalDailyStream,
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class),
				jsonSerdeRegistry.getSerde(SignalRecord.class))
		
		// into a stream of TransactionRecords<SignalRecords> per symbol and day (of the SignalRecord ending the transaction)
		// the TransactionRecords contain two SignalRecords starting and ending the transaction
		.<String, SignalRecord> transaction()
			.startsWith(
					(key, value) ->
						value.getSignalType().is(SignalType.Type.ENTRY))
			.endsWith(
					(key, value) ->
						value.getSignalType().is(SignalType.Type.EXIT))
			.groupBy(
					(key, value) ->
						value.getKey().getSymbol() + "-" + value.getStrategyKey(), 
						jsonSerdeRegistry.getSerde(String.class))
			.emit(
					EmitType.START_AND_END)
			.as(
					jsonSerdeRegistry.getSerde(
							(Class<TransactionRecord<String, SignalRecord>>)
							(Class<?>)
							TransactionRecord.class))
		
		// into stream of SignalRecords per symbol and day in sequential semantical
		// groups of [ENTRY, ONGOING, ..., ONGOING, EXIT] records whereas those
		// groups form a continuous group of days
		.<SymbolTimestampKey, SignalRecord> transform()
			.newKeyValues(
					(key, value) -> SignalExecutionProcessor.createContinuousDailySignalRecords(value))
			.asKeyValueType(
					jsonSerdeRegistry.getSerde(SymbolTimestampKey.class),
					jsonSerdeRegistry.getSerde(SignalRecord.class))
		
		// into same stream as above but keys shifted by one day
		.<SymbolTimestampKey, SignalRecord> transform()
			.changeKey(
					(key, value) -> {
						SymbolTimestampKey newKey = key.deepClone();
						newKey.setTimestamp(key.getTimestamp() + 86400000);
						return newKey;
					})
			.asKeyType(
					jsonSerdeRegistry.getSerde(SymbolTimestampKey.class))
		
		// the transforms above modify/create new records from the transaction
		// we need to put the records into the right time to be able to join
		// them with ohlcv_daily
		.adjustRecordTimestamps(
				(key, value) -> key.getTimestamp())
		
		// change the key to be the symbol only so that we can match with 
		// later dates
		.<String, SignalRecord>transform()
			.changeKey(
					(key, value) -> 
						key.getSymbol())
			.asKeyType(
					jsonSerdeRegistry.getSerde(String.class))
		
		// pass them through an intermediate topic for repartitioning and
		// debugging
		.through(TOPIC_SIGNAL_DAILY_EXPANDED)
		
		// into stream of SignalExecutionRecords[ENTRY|ONGOING|EXIT] from 
		// joined with stream ohlcv_daily
		// we get multiple joins (max 7) for each SignalRecord because of the
		// windowSizeAfter 7 days
		.<OHLCVRecord, SignalExecutionRecord> join(
				ohlcvRekeyed, 
				jsonSerdeRegistry.getSerde(OHLCVRecord.class))
		
			.withWindowSizeAfter(Duration.ofDays(7))
			.withRetentionPeriod(Duration.ofMillis(this.retentionMs + 86400000L)) // we add a day to have today access to the full retention time (record create ts is start of day)
			.as(
					SignalExecutionRecord::from, 
					jsonSerdeRegistry.getSerde(SignalExecutionRecord.class))
		
		// into stream containing only the first SignalExecutionRecords per 
		// SignalRecord
		.<SignalRecord,Void> dedup()
			.groupBy(
					(key, record) -> 
						record.getSignalRecord(),
					jsonSerdeRegistry.getSerde(SignalRecord.class))
			.emitFirst()
		
		// rekey back from symbol to SymbolTimestampKey
		.<SymbolTimestampKey, SignalExecutionRecord>transform()
			.changeKey(
					(key, value) -> 
						value.getKey())
			.asKeyType(
					jsonSerdeRegistry.getSerde(SymbolTimestampKey.class))
		
		// ensure correct record times
		.adjustRecordTimestamps(
				(key, value) -> key.getTimestamp())
		
		// to signal_execution_daily
		.to(
				BacktestStreamsFacade.TOPIC_SIGNAL_EXECUTION_DAILY);
	}
}
