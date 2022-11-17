package de.tradingpulse.optimus.analytics;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import de.tradingpulse.optimus.Trade;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RevRatioFilter implements Predicate<Trade>{

	private final double revRatioBarrier;

	public List<Trade> filter(List<Trade> trades) {
		return trades.stream()
				.filter(this)
				.collect(Collectors.toList());
	}
	
	@Override
	public boolean test(Trade t) {
		return t.getRevRatio() >= revRatioBarrier;
	}
}
