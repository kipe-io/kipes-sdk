package de.tradingpulse.optimus.analytics;

import java.util.Collection;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import de.tradingpulse.optimus.Trade;
import lombok.AllArgsConstructor;

/**
 * Score explained:
 * <pre>
 * - score = sum(revRatio)
 * - given we don't have risk function which limits the amount invested
 *   into a specific trade
 * - then each trade should be of the same amount
 * - then each trade entry value can be normalized to 1
 * - then revRatio == revenue_normalized
 *   - rev = exit - entry
 *   - revRatio = rev / entry    # entry == 1
 * - then score == sum(revenue_normalized)
 * - goal is to maximize revenue
 * </pre>
 * 
 */
public class Score {
	
	public static Stats stats(Collection<Trade> trades) {
		DescriptiveStatistics ds = new DescriptiveStatistics();
		
		for(Trade t: trades) {
			double revRatio = t.getRevRatio();
			ds.addValue(revRatio);
		}
		
		return new Stats(ds);
	}
	
	private Score() {}
	
	@AllArgsConstructor
	public static class Stats {
		DescriptiveStatistics ds = new DescriptiveStatistics();
		
		public long getCount() {
			return ds.getN();
		}
		
		public double getScore() {
			return ds.getSum();
		}
		
		public double getMinRevRatio() {
			return ds.getMin();
		}
		
		public double getMaxRevRatio() {
			return ds.getMax();
		}
		
		public double getMean() {
			return ds.getMean();
		}
		
		public double getAvgRev() {
			return getScore()/getCount();
		}
		
		public String toString() {
			return String.format("%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f", 
					getCount(),
					getScore(),
					getMinRevRatio(),
					ds.getPercentile(33),
					getMaxRevRatio(),
					getAvgRev());
		}
	}
}
