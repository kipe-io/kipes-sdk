package de.tradingpulse.stage.tradingscreens.recordtypes;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SwingSignalType.MOMENTUM;

import de.tradingpulse.common.stream.recordtypes.TradingDirection;

public enum ExitSignal {
	
	EXIT_SHORT_MOMENTUM(SHORT, MOMENTUM),
	EXIT_SHORT(SHORT, null),
	EXIT_LONG_MOMENTUM(LONG, MOMENTUM),
	EXIT_LONG(LONG, null);
	
	private final TradingDirection tradingDirection;
	private final SwingSignalType swingSignalType;
	
	private ExitSignal(TradingDirection tradingDirection, SwingSignalType swingSignalType) {
		this.tradingDirection = tradingDirection;
		this.swingSignalType = swingSignalType;
	}
	
	public boolean is(TradingDirection tradingDirection) {
		return this.tradingDirection == tradingDirection;
	}
	
	public boolean is(SwingSignalType swingSignalType) {
		return this.swingSignalType == null || this.swingSignalType == swingSignalType;
	}
}
