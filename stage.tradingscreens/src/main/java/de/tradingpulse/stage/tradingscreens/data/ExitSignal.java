package de.tradingpulse.stage.tradingscreens.data;

import static de.tradingpulse.stage.tradingscreens.data.SwingSignalType.MOMENTUM;

public enum ExitSignal {
	
	EXIT_SHORT_MOMENTUM(MOMENTUM),
	EXIT_SHORT(null),
	EXIT_LONG_MOMENTUM(MOMENTUM),
	EXIT_LONG(null);
	
	private final SwingSignalType swingSignalType;
	
	private ExitSignal(SwingSignalType swingSignalType) {
		this.swingSignalType = swingSignalType;
	}
	
	public boolean is(SwingSignalType swingSignalType) {
		return this.swingSignalType == null || this.swingSignalType == swingSignalType;
	}
}
