package de.tradingpulse.stage.tradingscreens.recordtypes;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;

import de.tradingpulse.common.stream.recordtypes.TradingDirection;

public enum SignalType {
	ENTRY_SHORT(Type.ENTRY, SHORT),
	ENTRY_LONG(Type.ENTRY, LONG),
	EXIT_SHORT(Type.EXIT, SHORT),
	EXIT_LONG(Type.EXIT, LONG);
	
	private final Type type;
	private final TradingDirection tradingDirection;
	
	private SignalType(Type type, TradingDirection tradingDirection) {
		this.type = type;
		this.tradingDirection = tradingDirection;
	}
	
	public boolean is(Type type) {
		return this.type == type;
	}
	
	public boolean is(TradingDirection tradingDirection) {
		return this.tradingDirection == tradingDirection;
	}
	
	public TradingDirection getTradingDirection() {
		return this.tradingDirection;
	}
	
	public enum Type {
		ENTRY,
		EXIT;
	}
}

