package de.tradingpulse.optimus.analytics;

import static de.tradingpulse.optimus.analytics.TrendPredicateFactory.createTrendPredicate;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import de.tradingpulse.optimus.Trade;
import de.tradingpulse.optimus.analytics.TradeFilter.DirectionPredicate;
import de.tradingpulse.optimus.analytics.TradeFilter.TrendsPredicate;
import de.tradingpulse.stage.systems.recordtypes.Trends;

public class TradeFilters {
	
	public static List<TradeFilter> createNStrategyFilters(List<Trade> trades) {
		return dedup(
				trades.stream().parallel()
					.map(trade -> {
						
						return TradeFilter.builder()
								.direction(new DirectionPredicate(trade.getTradingDirection()))
								.longTrends(createTrendsPredicate(trade.getLongRangeTrends()))
								.shortTrends(createTrendsPredicate(trade.getShortRangeTrends()))
								.build();
					
					})
					.collect(Collectors.toList())
				);
	}

	private static TrendsPredicate createTrendsPredicate(Trends trends) {
		return TrendsPredicate.builder()
				.ema(createTrendPredicate(trends.getEma()))
				
				.macdHistogram(createTrendPredicate(trends.getMacdHistogram()))
				.macdLinesSlope(createTrendPredicate(trends.getMacdLinesSlope()))
				.macdValue(createTrendPredicate(trends.getMacdValue()))
				
				.sstocSlope(createTrendPredicate(trends.getSstocSlope()))
				.sstocValue(createTrendPredicate(trends.getSstocValue()))
				
				.build();
	}

	public static List<TradeFilter> dedup(List<TradeFilter> filters) {
		List<TradeFilter> deduped = new LinkedList<>();
		
		filters.forEach(filter -> {
			if(!deduped.contains(filter)) {
				deduped.add(filter);
			}
		});
		
		return deduped;
	}

	public static TradeFilter createCombinedFilter(List<TradeFilter> filters) {
		TradeFilter combinedFilter = new TradeFilter();
		
		filters.forEach(combinedFilter::merge);
		
		return combinedFilter;
	}
	
	public static List<Trade> filter(List<Trade> trades, List<TradeFilter> filters) {
		return trades.stream().parallel()
				.filter(trade -> {
					for(TradeFilter filter: filters) {
						if(filter.test(trade)) {
							return true;
						}
					}
					
					return false;
				})
				.collect(Collectors.toList());
	}
	
	public static void printFilterSummary(List<TradeFilter> filters) {
		TradeFilters.createCombinedFilter(filters).print();
	}

	private TradeFilters() {}
}
