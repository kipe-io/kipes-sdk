package de.tradingpulse.stage.backtest.service.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TimeRange;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType;
import de.tradingpulse.streams.recordtypes.TransactionRecord;

class SignalExecutionProcessorTest {
	
	// ------------------------------------------------------------------------
	// createContinuousDailySignalRecords
	// ------------------------------------------------------------------------

	private static final String STRATEGY = "strategy";
	private static final String SYMBOL = "symbol";

	@Test
	void test_createContinuousDailySignalRecords__no_days_between_ENTRY_and_EXIT__returns_two() {
		TransactionRecord<String, SignalRecord> r = createTransactionRecord(0, 86400000);
		
		List<KeyValue<SymbolTimestampKey, SignalRecord>> kvs = SignalExecutionProcessor.createContinuousDailySignalRecords(r);
		
		assertEquals(2, kvs.size());
		assertEquals(SignalType.ENTRY_LONG, kvs.get(0).value.getSignalType());
		assertEquals(SignalType.EXIT_LONG, kvs.get(1).value.getSignalType());
	}

	@Test
	void test_createContinuousDailySignalRecords__one_day_between_ENTRY_and_EXIT__returns_three() {
		TransactionRecord<String, SignalRecord> r = createTransactionRecord(0, 2 * 86400000);
		
		List<KeyValue<SymbolTimestampKey, SignalRecord>> kvs = SignalExecutionProcessor.createContinuousDailySignalRecords(r);
		
		assertEquals(3, kvs.size());
		assertEquals(SignalType.ENTRY_LONG, kvs.get(0).value.getSignalType());
		assertEquals(SignalType.ONGOING_LONG, kvs.get(1).value.getSignalType());
		assertEquals(SignalType.EXIT_LONG, kvs.get(2).value.getSignalType());
	}

	@Test
	void test_createContinuousDailySignalRecords__ongoing_signals_are_correct() {
		TransactionRecord<String, SignalRecord> r = createTransactionRecord(0, 2 * 86400000);
		
		List<KeyValue<SymbolTimestampKey, SignalRecord>> kvs = SignalExecutionProcessor.createContinuousDailySignalRecords(r);
		
		KeyValue<SymbolTimestampKey, SignalRecord> kv = kvs.get(1);
		
		assertEquals(SYMBOL, kv.key.getSymbol());
		assertEquals(86400000, kv.key.getTimestamp());
		
		assertEquals(SYMBOL, kv.value.getKey().getSymbol());
		assertEquals(86400000, kv.value.getKey().getTimestamp());
		assertEquals(TimeRange.DAY, kv.value.getTimeRange());
		assertEquals(SignalType.ONGOING_LONG, kv.value.getSignalType());
		assertEquals(STRATEGY, kv.value.getStrategyKey());
	}
	
	/**
	 * <pre>
	 * 	[ ENTRY_LONG, EXIT_LONG ]
	 * </pre>
	 */
	private TransactionRecord<String, SignalRecord> createTransactionRecord(long entryMs, long exitMs) {
		TransactionRecord<String, SignalRecord> r = new TransactionRecord<>();
		
		r.setKey(new SymbolTimestampKey(SYMBOL, entryMs));
		r.addUnique(createSignalRecord(SignalType.ENTRY_LONG, entryMs));
		r.addUnique(createSignalRecord(SignalType.EXIT_LONG, exitMs));
		return r;
	}

	private SignalRecord createSignalRecord(SignalType signalType, long ms) {
		return SignalRecord.builder()
				.key(new SymbolTimestampKey(SYMBOL, ms))
				.timeRange(TimeRange.DAY)
				.signalType(signalType)
				.strategyKey(STRATEGY)
				.build();
	}
}
