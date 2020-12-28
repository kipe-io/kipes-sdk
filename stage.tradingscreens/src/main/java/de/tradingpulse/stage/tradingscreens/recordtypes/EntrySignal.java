package de.tradingpulse.stage.tradingscreens.recordtypes;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SwingSignalType.SWING_MARKET_TURN_POTENTIAL;

import de.tradingpulse.common.stream.recordtypes.TradingDirection;

public enum EntrySignal {
	ENTRY_SHORT_MOMENTUM(SHORT, null),
	ENTRY_SHORT_POTENTIAL(SHORT, SWING_MARKET_TURN_POTENTIAL),
	ENTRY_LONG_MOMENTUM(LONG, null),
	ENTRY_LONG_POTENTIAL(LONG, SWING_MARKET_TURN_POTENTIAL);
	
	private final TradingDirection tradingDirection;
	private final SwingSignalType swingSignalType;
	
	private EntrySignal(TradingDirection tradingDirection, SwingSignalType swingSignalType) {
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
