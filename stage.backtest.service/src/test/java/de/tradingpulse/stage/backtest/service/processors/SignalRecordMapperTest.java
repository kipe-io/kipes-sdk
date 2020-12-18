package de.tradingpulse.stage.backtest.service.processors;

import static de.tradingpulse.stage.backtest.recordtypes.SignalType.ENTRY_LONG;
import static de.tradingpulse.stage.backtest.recordtypes.SignalType.ENTRY_SHORT;
import static de.tradingpulse.stage.backtest.recordtypes.SignalType.EXIT_LONG;
import static de.tradingpulse.stage.backtest.recordtypes.SignalType.EXIT_SHORT;
import static de.tradingpulse.stage.backtest.service.processors.SignalRecordMapper.SWING_MARKET_TURN;
import static de.tradingpulse.stage.backtest.service.processors.SignalRecordMapper.SWING_MOMENTUM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.recordtypes.SignalRecord;
import de.tradingpulse.stage.backtest.recordtypes.SignalType;
import de.tradingpulse.stage.tradingscreens.recordtypes.EntrySignal;
import de.tradingpulse.stage.tradingscreens.recordtypes.ExitSignal;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseSignalRecord;

class SignalRecordMapperTest {

	@ParameterizedTest
	@MethodSource("applyTestData")
	void test_apply(
			EntrySignal entry,
			ExitSignal exit,
			SignalRecord[] signals)
	{
		ImpulseSignalRecord value = ImpulseSignalRecord.builder()
				.key(new SymbolTimestampKey())
				.entry(entry)
				.exit(exit)
				.build();
		
		SignalRecordMapper mapper = new SignalRecordMapper();
		List<KeyValue<SymbolTimestampKey, SignalRecord>> signalKVs = 
				(List<KeyValue<SymbolTimestampKey, SignalRecord>>)mapper.apply(null, value);
		
		assertEquals(signals.length, signalKVs.size());
		int i = 0;
		for(SignalRecord expectedSignal : signals) {
			// note that we are assuming same order, not necessarily a required feature
			SignalRecord actualSignal = signalKVs.get(i).value;
			
			assertEquals(expectedSignal.getSignalType(), actualSignal.getSignalType());
			assertEquals(expectedSignal.getStrategyKey(), actualSignal.getStrategyKey());
			
			i++;
		}
	}

	static Stream<Arguments> applyTestData() {
		return Stream.of(
				arguments(null, 								ExitSignal.EXIT_LONG_MOMENTUM,	new SignalRecord[]{ exitLongMomentum() }),
				arguments(null, 								ExitSignal.EXIT_LONG,			new SignalRecord[]{ exitLongMomentum(),		exitLongTurn() }),
				arguments(null, 								ExitSignal.EXIT_SHORT_MOMENTUM,	new SignalRecord[]{ exitShortMomentum() }),
				arguments(null, 								ExitSignal.EXIT_SHORT,			new SignalRecord[]{ exitShortMomentum(),	exitShortTurn() }),
				
				arguments(EntrySignal.ENTRY_LONG_POTENTIAL,		ExitSignal.EXIT_LONG_MOMENTUM,	new SignalRecord[]{ exitLongMomentum() }),
				arguments(EntrySignal.ENTRY_LONG_POTENTIAL,		ExitSignal.EXIT_LONG,			new SignalRecord[]{ exitLongMomentum(),		exitLongTurn() }), 
				arguments(EntrySignal.ENTRY_LONG_POTENTIAL,		ExitSignal.EXIT_SHORT_MOMENTUM,	new SignalRecord[]{ exitShortMomentum(),						entryLongTurn() }),
				arguments(EntrySignal.ENTRY_LONG_POTENTIAL,		ExitSignal.EXIT_SHORT,			new SignalRecord[]{	exitShortMomentum(),	exitShortTurn(),	entryLongTurn() }),
				arguments(EntrySignal.ENTRY_LONG_POTENTIAL,		null,							new SignalRecord[]{												entryLongTurn() }),
				
				arguments(EntrySignal.ENTRY_LONG_MOMENTUM,		ExitSignal.EXIT_LONG_MOMENTUM,	new SignalRecord[]{ exitLongMomentum() }),
				arguments(EntrySignal.ENTRY_LONG_MOMENTUM,		ExitSignal.EXIT_LONG,			new SignalRecord[]{ exitLongMomentum(),		exitLongTurn() }), 
				arguments(EntrySignal.ENTRY_LONG_MOMENTUM,		ExitSignal.EXIT_SHORT_MOMENTUM,	new SignalRecord[]{ exitShortMomentum(),						entryLongMomentum(),	entryLongTurn() }),
				arguments(EntrySignal.ENTRY_LONG_MOMENTUM,		ExitSignal.EXIT_SHORT,			new SignalRecord[]{	exitShortMomentum(),	exitShortTurn(),	entryLongMomentum(),	entryLongTurn() }),
				arguments(EntrySignal.ENTRY_LONG_MOMENTUM,		null,							new SignalRecord[]{												entryLongMomentum(),	entryLongTurn() }),
				
				arguments(EntrySignal.ENTRY_SHORT_POTENTIAL,	ExitSignal.EXIT_LONG_MOMENTUM,	new SignalRecord[]{ exitLongMomentum(),							entryShortTurn() }),
				arguments(EntrySignal.ENTRY_SHORT_POTENTIAL,	ExitSignal.EXIT_LONG,			new SignalRecord[]{ exitLongMomentum(),		exitLongTurn(),		entryShortTurn() }), 
				arguments(EntrySignal.ENTRY_SHORT_POTENTIAL,	ExitSignal.EXIT_SHORT_MOMENTUM,	new SignalRecord[]{ exitShortMomentum() }),
				arguments(EntrySignal.ENTRY_SHORT_POTENTIAL,	ExitSignal.EXIT_SHORT,			new SignalRecord[]{	exitShortMomentum(),	exitShortTurn() }),
				arguments(EntrySignal.ENTRY_SHORT_POTENTIAL,	null,							new SignalRecord[]{												entryShortTurn() }),
				
				arguments(EntrySignal.ENTRY_SHORT_MOMENTUM,		ExitSignal.EXIT_LONG_MOMENTUM,	new SignalRecord[]{ exitLongMomentum(),							entryShortMomentum(),	entryShortTurn() }),
				arguments(EntrySignal.ENTRY_SHORT_MOMENTUM,		ExitSignal.EXIT_LONG,			new SignalRecord[]{ exitLongMomentum(),		exitLongTurn(),		entryShortMomentum(),	entryShortTurn() }), 
				arguments(EntrySignal.ENTRY_SHORT_MOMENTUM,		ExitSignal.EXIT_SHORT_MOMENTUM,	new SignalRecord[]{ exitShortMomentum() }),
				arguments(EntrySignal.ENTRY_SHORT_MOMENTUM,		ExitSignal.EXIT_SHORT,			new SignalRecord[]{	exitShortMomentum(),	exitShortTurn() }),
				arguments(EntrySignal.ENTRY_SHORT_MOMENTUM,		null,							new SignalRecord[]{												entryShortMomentum(),	entryShortTurn() }),
				
				arguments(null, 								null, 							new SignalRecord[]{})
			);
	}
	
	
	static SignalRecord exitLongMomentum() {
		return createSignalRecord(EXIT_LONG, SWING_MOMENTUM);
	}
	
	static SignalRecord exitLongTurn() {
		return createSignalRecord(EXIT_LONG, SWING_MARKET_TURN);
	}
	
	static SignalRecord exitShortMomentum() {
		return createSignalRecord(EXIT_SHORT, SWING_MOMENTUM);
	}
	
	static SignalRecord exitShortTurn() {
		return createSignalRecord(EXIT_SHORT, SWING_MARKET_TURN);
	}
	
	static SignalRecord entryLongMomentum() {
		return createSignalRecord(ENTRY_LONG, SWING_MOMENTUM);
	}
	
	static SignalRecord entryLongTurn() {
		return createSignalRecord(ENTRY_LONG, SWING_MARKET_TURN);
	}
	
	static SignalRecord entryShortMomentum() {
		return createSignalRecord(ENTRY_SHORT, SWING_MOMENTUM);
	}
	
	static SignalRecord entryShortTurn() {
		return createSignalRecord(ENTRY_SHORT, SWING_MARKET_TURN);
	}
	
	static SignalRecord createSignalRecord(SignalType signalType, String strategyKey) {
		return SignalRecord.builder()
				.signalType(signalType)
				.strategyKey(strategyKey)
				.build();
	}
}
