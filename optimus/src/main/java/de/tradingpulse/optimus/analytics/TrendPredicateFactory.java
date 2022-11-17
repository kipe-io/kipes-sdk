package de.tradingpulse.optimus.analytics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.optimus.analytics.TradeFilter.TrendsPredicate.TrendPredicate;

public class TrendPredicateFactory {

	private final static Map<TradingDirection[], TrendPredicate> CACHE = new HashMap<>();
	
	public static TrendPredicate createTrendPredicate(String direction) {
		return createTrendPredicate(TradingDirection.valueOf(direction));
	}
	
	public static synchronized TrendPredicate createTrendPredicate(TradingDirection...direction) {
		Arrays.sort(direction);
		return CACHE.computeIfAbsent(direction, TrendPredicate::new);
	}
	
	private TrendPredicateFactory() {}
}
