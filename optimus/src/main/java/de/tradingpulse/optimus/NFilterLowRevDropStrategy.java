package de.tradingpulse.optimus;

import static de.tradingpulse.optimus.analytics.Score.stats;
import static java.lang.String.format;

import java.util.List;
import java.util.stream.Collectors;

import de.tradingpulse.optimus.analytics.RevRatioFilter;
import de.tradingpulse.optimus.analytics.Score.Stats;
import de.tradingpulse.optimus.analytics.TradeFilter;
import de.tradingpulse.optimus.analytics.TradeFilters;

public class NFilterLowRevDropStrategy {

	public static void process(List<Trade> tradesAll) {
		System.out.println(format("TRADE_COUNT_ALL    : %d", tradesAll.size()));
		System.out.println(format("--------------------"));
		
		
		
		// 1) given a REV_RATIO_BARRIER
		double revRatioBarrier = 0.2;
		RevRatioFilter revRatioBarrierFilter = new RevRatioFilter(revRatioBarrier); // evaluate somehow the best barrier of the give trades
		
		// 2) build TRADES_REV_BARRIER including all trades with revRatio >= REV_RATIO_BARRIER 
		//    - given the SCORE := sum(revRatio) 
		//    - SCORE_INITIAL := SCORE(TRADES_REV_BARRIER)
		//    - TRADE_COUNT_INITIAL := TRADES_REV_BARRIER.count
		
		List<Trade> tradesRevBarrier = tradesAll; //revRatioBarrierFilter.filter(tradesAll);
		
		final double scoreInitial = stats(tradesRevBarrier).getScore();
		final int tradeCountInitial = tradesRevBarrier.size();
		
		System.out.println(format("REV_RATIO_BARRIER  : %.2f", revRatioBarrier));
		System.out.println(format("TRADE_COUNT_INITIAL: %d", tradeCountInitial));
		System.out.println(format("SCORE_INITIAL      : %.2f", scoreInitial));
		System.out.println(format("AVG_REVENUE_INITIAL: %.2f", scoreInitial/tradeCountInitial));
		System.out.println(format("===================="));
		
		// Now, the goal is to find one or multiple filters, so that
		// - score is maximal
		// - filter variants are minimal
		// - trade count is minimal
		//
		//
		// N FILTER STRATEGY
		// - for each trade of TRADES_REV_RATIO_INITIAL build a dedicated filter: FILTERS
		
		List<TradeFilter> filters = TradeFilters.createNStrategyFilters(tradesRevBarrier);
		
		// - iterate:
		//   - for each filter from FILTERS remove equal filters
		//   - FILTER_VARIANTS := FILTERS.count
		//   - TRADES_FILTERED := filter(TRADES, FILTERS)
		//   - SCORE_FILTERED := SCORE(TRADES_FILTERED)
		//   - TRADES_FILTERED_COUNT := TRADES_FILTERED.count

		int iteration = 0;
		System.out.println("iteration\tfilters\ttrades\tt_f_ratio\tscore\tavg_rev");
		String format = "%d\t%d\t%d\t%.2f\t%.2f\t%.2f";
		while( true ) {
			iteration++;
			
			List<Trade> tradesFiltered = TradeFilters.filter(tradesAll, filters); 
			final double score = stats(tradesFiltered).getScore();
			final int tradesCount = tradesFiltered.size();
			final double avgRev = score/tradesCount;
			System.out.println(format(format,
					iteration,
					filters.size(),
					tradesCount,
					((double)tradesCount)/filters.size(),
					score,
					avgRev));
			
			if(avgRev >= revRatioBarrier) {
				break;
			}
			
			//   - when SCORE_FILTERED < SCORE_INITIAL
			//     - remove the filter with the highest count of trades < REV_RATIO_BARRIER
			//
			
			TradeFilter candidate = null;
			double candidateAvgRev = 0.0;
			
			for(TradeFilter filter : filters ) {
				Stats filterStats = stats(filter.getPassingObjects());
				filter.clear();
				
				if(	filterStats.getAvgRev() > revRatioBarrier 
					|| candidate != null && filterStats.getAvgRev() > candidateAvgRev) 
				{
					continue;
				}
			
				candidate = filter;
				candidateAvgRev = filterStats.getAvgRev(); 
			}
			
			if(candidate == null) {
				break;
			}

			filters.remove(candidate);
			
		}		
	}
}
