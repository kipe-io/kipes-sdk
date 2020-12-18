package de.tradingpulse.stage.tradingscreens.data;

import static de.tradingpulse.stage.tradingscreens.data.SwingSignalType.MARKET_TURN_POTENTIAL;
import static de.tradingpulse.stage.tradingscreens.data.SwingSignalType.MOMENTUM;

public enum EntrySignal {
	ENTRY_SHORT_MOMENTUM(MOMENTUM),
	ENTRY_SHORT_POTENTIAL(MARKET_TURN_POTENTIAL),
	ENTRY_LONG_MOMENTUM(MOMENTUM),
	ENTRY_LONG_POTENTIAL(MARKET_TURN_POTENTIAL);
	
	private final SwingSignalType swingSignalType;
	
	private EntrySignal(SwingSignalType swingSignalType) {
		this.swingSignalType = swingSignalType;
	}
	
	public boolean is(SwingSignalType swingSignalType) {
		return this.swingSignalType == swingSignalType;
	}
}
