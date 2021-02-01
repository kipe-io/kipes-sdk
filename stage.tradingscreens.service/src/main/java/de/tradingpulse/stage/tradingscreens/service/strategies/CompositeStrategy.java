package de.tradingpulse.stage.tradingscreens.service.strategies;

import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;

public class CompositeStrategy
implements BiFunction<SymbolTimestampKey, ImpulseTradingScreenRecord, Iterable<SignalRecord>> {

	private final String strategyKey;
	private final BiFunction<SymbolTimestampKey, ImpulseTradingScreenRecord, Iterable<SignalRecord>> strategyOne;
	private final BiFunction<SymbolTimestampKey, ImpulseTradingScreenRecord, Iterable<SignalRecord>> strategyTwo;
	
	public CompositeStrategy(
			String strategyKey, 
			BiFunction<SymbolTimestampKey, ImpulseTradingScreenRecord, Iterable<SignalRecord>> strategyOne,
			BiFunction<SymbolTimestampKey, ImpulseTradingScreenRecord, Iterable<SignalRecord>> strategyTwo)
	{
		this.strategyKey = strategyKey;
		this.strategyOne = strategyOne;
		this.strategyTwo = strategyTwo;
	}
	
	
	@Override
	public Iterable<SignalRecord> apply(SymbolTimestampKey key, ImpulseTradingScreenRecord value) {
		List<SignalRecord> signals = new LinkedList<>();
		
		this.strategyOne.apply(key, value).forEach(signal -> {
			SignalRecord copy = signal.deepClone();
			copy.setStrategyKey(this.strategyKey);
			signals.add(copy);
		});
		
		this.strategyTwo.apply(key, value).forEach(signal -> {
			SignalRecord copy = signal.deepClone();
			copy.setStrategyKey(this.strategyKey);
			signals.add(copy);
		});
		
		return signals;
	}
}
