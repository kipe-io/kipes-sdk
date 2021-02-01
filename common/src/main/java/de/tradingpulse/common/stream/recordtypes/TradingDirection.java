package de.tradingpulse.common.stream.recordtypes;

public enum TradingDirection {
	SHORT,
	NEUTRAL,
	LONG;
	
	public static TradingDirection getChangeDirection(TradingDirection from, TradingDirection to) {
		switch(from) {
		case SHORT:
			switch(to) {
			case SHORT: return NEUTRAL;
			case NEUTRAL: return LONG;
			case LONG: return LONG;
			}
			break;
		case NEUTRAL:
			switch(to) {
			case SHORT: return SHORT;
			case NEUTRAL: return NEUTRAL;
			case LONG: return LONG;
			}
			break;
		case LONG:
			switch(to) {
			case SHORT: return SHORT;
			case NEUTRAL: return SHORT;
			case LONG: return NEUTRAL;
			}
		}
		
		throw new UnsupportedOperationException("unknown combination: "+from+", "+to);
	}

	public TradingDirection opposite() {
		switch(this) {
		case SHORT:	return LONG;
		case NEUTRAL : return NEUTRAL;
		case LONG: return SHORT;
		}
		
		throw new UnsupportedOperationException("unknown case: "+this);		
	}
}