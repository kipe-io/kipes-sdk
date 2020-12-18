package de.tradingpulse.stage.backtest.service.processors;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static de.tradingpulse.stage.backtest.recordtypes.SignalType.ENTRY_LONG;
import static de.tradingpulse.stage.backtest.recordtypes.SignalType.ENTRY_SHORT;
import static de.tradingpulse.stage.backtest.recordtypes.SignalType.EXIT_LONG;
import static de.tradingpulse.stage.backtest.recordtypes.SignalType.EXIT_SHORT;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.recordtypes.SignalRecord;
import de.tradingpulse.stage.backtest.recordtypes.SignalType;
import de.tradingpulse.stage.tradingscreens.recordtypes.EntrySignal;
import de.tradingpulse.stage.tradingscreens.recordtypes.ExitSignal;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseSignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SwingSignalType;

public class SignalRecordMapper 
implements KeyValueMapper<SymbolTimestampKey, ImpulseSignalRecord, Iterable<KeyValue<SymbolTimestampKey, SignalRecord>>>
{
	static final String SWING_MARKET_TURN = "SWING_MARKET_TURN";
	static final String SWING_MOMENTUM = "SWING_MOMENTUM";
	
	private static final Logger LOG = LoggerFactory.getLogger(SignalRecordMapper.class);
	
	@Override
	public Iterable<KeyValue<SymbolTimestampKey, SignalRecord>> apply(
			SymbolTimestampKey key,
			ImpulseSignalRecord value)
	{
		List<SignalRecord> signals = createExitSignals(value);
		signals.addAll(createEntrySignals(value));
		
		// map to KeyValues
		return signals.stream()
				.map(signal -> new KeyValue<>(signal.getKey(), signal))
				.collect(Collectors.toList());
	}
	
	private List<SignalRecord> createExitSignals(ImpulseSignalRecord value) {
		ExitSignal exit = value.getExit();
		List<SignalRecord> exitSignals = new LinkedList<>();
		
		if(exit == null) {
			return exitSignals;
		}
		
		SignalType signalType = exit.is(SHORT)? EXIT_SHORT : EXIT_LONG;
		
		if(exit.is(SwingSignalType.MOMENTUM)) {
			exitSignals.add(SignalRecord.builder()
					.key(value.getKey().deepClone())
					.timeRange(value.getTimeRange())
					.signalType(signalType)
					.strategyKey(SWING_MOMENTUM)
					.build());
		}
		
		if(exit.is(SwingSignalType.MARKET_TURN_POTENTIAL)) {
			exitSignals.add(SignalRecord.builder()
					.key(value.getKey().deepClone())
					.timeRange(value.getTimeRange())
					.signalType(signalType)
					.strategyKey(SWING_MARKET_TURN)
					.build());
		}
		
		return exitSignals;
	}
	
	private List<SignalRecord> createEntrySignals(ImpulseSignalRecord value) {
		EntrySignal entry = value.getEntry();
		List<SignalRecord> entrySignals = new LinkedList<>();
		
		if(entry == null) {
			return entrySignals;
		}
		
		SignalType signalType = entry.is(SHORT)? ENTRY_SHORT : ENTRY_LONG;
		
		// if there's the strange case that we got both an entry and exit signal
		// for the same TradingDirection we ignore the entry
		ExitSignal exit = value.getExit();
		if(exit != null && exit.is(signalType.getTradingDirection())) {
			LOG.warn("Found both exit and entry signal for the same trading direction. Check your strategy! Ignoring entry signal. {}", value);
			return entrySignals;
		}
		
		if(entry.is(SwingSignalType.MOMENTUM)) {
			entrySignals.add(SignalRecord.builder()
					.key(value.getKey().deepClone())
					.timeRange(value.getTimeRange())
					.signalType(signalType)
					.strategyKey(SWING_MOMENTUM)
					.build());
		}
		
		if(entry.is(SwingSignalType.MARKET_TURN_POTENTIAL)) {
			entrySignals.add(SignalRecord.builder()
					.key(value.getKey().deepClone())
					.timeRange(value.getTimeRange())
					.signalType(signalType)
					.strategyKey(SWING_MARKET_TURN)
					.build());
		}
		
		return entrySignals;
	}
}
