package de.tradingpulse.stage.tradingscreens.service.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.recordtypes.EntrySignal;
import de.tradingpulse.stage.tradingscreens.recordtypes.ExitSignal;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseSignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SwingSignalType;

class ImpulseTradingScreenTransformer 
implements Transformer<SymbolTimestampKey, ImpulseTradingScreenRecord, KeyValue<SymbolTimestampKey, ImpulseSignalRecord>> {

	private final SwingSignalType swingSignalType;
	
	ImpulseTradingScreenTransformer(SwingSignalType swingSignalType) {
		this.swingSignalType = swingSignalType;
	}
	
	@Override
	public void init(ProcessorContext context) {
		// nothing to do
	}

	@Override
	public KeyValue<SymbolTimestampKey, ImpulseSignalRecord> transform(
			SymbolTimestampKey key,
			ImpulseTradingScreenRecord value)
	{
		if (value.getEntrySignal().isEmpty() && value.getExitSignal().isEmpty()) {
			return null;
		}
		
		EntrySignal entry = value.getEntrySignal().isEmpty()? 
				null
				: value.getEntrySignal().get().is(this.swingSignalType) ? 
						value.getEntrySignal().get()
						: null;
		
		ExitSignal exit = value.getExitSignal().isEmpty()? 
				null
				: value.getExitSignal().get().is(this.swingSignalType) ? 
						value.getExitSignal().get()
						: null;
		
		if (entry == null && exit == null) {
			return null;
		}
		
		return new KeyValue<>(key, ImpulseSignalRecord.builder()
				.key(value.getKey())
				.timeRange(value.getTimeRange())
				.entry(entry)
				.exit(exit)
				.build());
	}

	@Override
	public void close() {
		// nothing to do
	}
}
