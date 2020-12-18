package de.tradingpulse.stage.backtest.recordtypes;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;

import de.tradingpulse.common.stream.recordtypes.TradingDirection;

public enum SignalType {
	ENTRY_SHORT(SHORT),
	ENTRY_LONG(LONG),
	EXIT_SHORT(SHORT),
	EXIT_LONG(LONG);
	
	private final TradingDirection tradingDirection;
	
	private SignalType(TradingDirection tradingDirection) {
		this.tradingDirection = tradingDirection;
	}
	
	public boolean is(TradingDirection tradingDirection) {
		return this.tradingDirection == tradingDirection;
	}
	
	public TradingDirection getTradingDirection() {
		return this.tradingDirection;
	}
}
