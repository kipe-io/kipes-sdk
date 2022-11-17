package de.tradingpulse.optimus;

import static java.lang.String.format;

import java.util.List;
import java.util.stream.Collectors;

import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.optimus.analytics.Score;
import de.tradingpulse.optimus.analytics.Score.Stats;
import de.tradingpulse.optimus.analytics.TradeFilter;
import de.tradingpulse.optimus.analytics.TradeFilters;
import de.tradingpulse.optimus.io.BacktestResultRecordReader;

public class Optimus {

	public static void main(String[] args) {
		System.out.println(format("FILE               : %s", args[0]));
		
		// to generate a matching file:
		// $ kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic stg-backtest-backtestresult_daily --from-beginning > <FILE_PATH>
		
		List<Trade> tradesAll = BacktestResultRecordReader.read(args[0]).stream()
				.map(Trade::from)
				.filter(trade -> trade.getTradingDirection() == TradingDirection.SHORT)
				.collect(Collectors.toList());
		
		// INITIAL FILTER IDEAS (means different strategies to optimize)
		// - one filter which matches all TRADES_REV_RATIO_INITIAL
		// - N filters each matching one trade of TRADES_REV_RATIO_INITIAL, 
		//   N <= TRADES_REV_RATIO_INITIAL.count
		
		
		// show trades / avgRev per filter ------------------------------------ 
		List<TradeFilter> filters = TradeFilters.createNStrategyFilters(tradesAll);
		
		// filter once to fill the CollectingFilter
		TradeFilters.filter(tradesAll, filters);
		
		System.out.println("filter\ttrades\tscore\tminRevR\tmeanRevR\tmaxRevR\tavgRevR\tdirection\tfilter");
		String format = "%d\t%s\t%s";
		int i = 0;
		for(TradeFilter filter :filters ) {
			i++;
			Stats filterStats = Score.stats(filter.getPassingObjects());
			
			System.out.println(String.format(format, 
					i,
					filterStats,
					filter));
		}
		
		//NFilterLowRevDropStrategy.process(tradesAll);
	}
}
