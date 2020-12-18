package de.tradingpulse.stage.backtest.service.processors;

import java.security.KeyException;
import java.security.PublicKey;
import java.time.Duration;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.xml.crypto.dsig.keyinfo.KeyValue;

import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.Stores;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.recordtypes.SignalRecord;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;

@Singleton
public class BacktestResultProcessor extends AbstractProcessorFactory {

	@Inject
	private SourceDataStreamsFacade sourceDataStreamsFacade;
	
	@Inject
	private BacktestStreamsFacade backtestStreamsFacade;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() throws Exception {
		// --------------------------------------------------------------------
		// from signal_daily
		// join ohlcv_daily window size 7 days
		// 
		// group by symbol, strategyKey
		// aggregate backtest_result 
		// - new_keep_existing on entry
		// - update, delete, emit on exit
		// to backtest_result_daily
		// --------------------------------------------------------------------
		
		String topicName = backtestStreamsFacade.getBacktestResultDailyStreamName();
		
		// setup join parameters
		// TODO externalize retention period
		// IDEA: (via AbstractStreamFactory.topics.XXX.retentionMs)
		// the config depends on the overall retention period and the earliest
		// day we fetch at the iexcloud connector. 
		final Duration storeRetentionPeriod = Duration.ofMillis(this.retentionMs + 86400000L); // we add a day to have today access to the full retention time (record create ts is start of day)
		final Duration windowSize = Duration.ofSeconds(0);
		final boolean retainDuplicates = true; // topology creation will fail on false
		
		backtestStreamsFacade.getSignalDailyStream()
		.join(
				sourceDataStreamsFacade.getOhlcvDailyStream(),
				
				(signalRecord, ohlcvRecord) -> null,
				
				// window size can be small as we know the data is at minimum at minute intervals
				JoinWindows
				.of(windowSize)
				.grace(storeRetentionPeriod),
				// configuration of the underlying window join stores for keeping the data
				StreamJoined.<SymbolTimestampKey, SignalRecord, OHLCVRecord>with(
						Stores.persistentWindowStore(
								topicName+"-join-store-left", 
								storeRetentionPeriod, 
								windowSize, 
								retainDuplicates), 
						Stores.persistentWindowStore(
								topicName+"-join-store-right", 
								storeRetentionPeriod, 
								windowSize, 
								retainDuplicates))
				.withKeySerde(jsonSerdeRegistry.getSerde(SymbolTimestampKey.class))
				.withValueSerde(jsonSerdeRegistry.getSerde(SignalRecord.class))
				.withOtherValueSerde(jsonSerdeRegistry.getSerde(OHLCVRecord.class)));
	}
}
