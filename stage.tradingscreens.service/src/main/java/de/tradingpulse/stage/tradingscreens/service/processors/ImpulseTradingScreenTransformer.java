package de.tradingpulse.stage.tradingscreens.service.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.data.EntrySignal;
import de.tradingpulse.stage.tradingscreens.data.ExitSignal;
import de.tradingpulse.stage.tradingscreens.data.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.data.SignalRecord;
import de.tradingpulse.stage.tradingscreens.data.SwingSignalType;

class ImpulseTradingScreenTransformer 
implements Transformer<SymbolTimestampKey, ImpulseTradingScreenRecord, KeyValue<SymbolTimestampKey, SignalRecord>> {

	private final SwingSignalType swingSignalType;
	
	ImpulseTradingScreenTransformer(SwingSignalType swingSignalType) {
		this.swingSignalType = swingSignalType;
	}
	
	@Override
	public void init(ProcessorContext context) {
		// nothing to do
	}

	@Override
	public KeyValue<SymbolTimestampKey, SignalRecord> transform(
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
		
		return new KeyValue<>(key, SignalRecord.builder()
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
