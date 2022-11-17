package de.tradingpulse.optimus.analytics;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.optimus.Trade;
import de.tradingpulse.stage.systems.recordtypes.Trends;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@Builder
@EqualsAndHashCode(callSuper = false)
public class TradeFilter extends CollectingFilter<Trade> {

	private final DirectionPredicate direction;
	private final TrendsPredicate longTrends;
	private final TrendsPredicate shortTrends;
		
	public TradeFilter() {
		this.direction = new DirectionPredicate();
		this.longTrends = new TrendsPredicate();
		this.shortTrends = new TrendsPredicate();
	}
	
	@Override
	public boolean doTest(Trade t) {
		return direction.test(t.getTradingDirection())
				&& longTrends.test(t.getLongRangeTrends())
				&& shortTrends.test(t.getShortRangeTrends());
	}

	public void merge(TradeFilter other) {
		this.direction.merge(other.direction);
		this.longTrends.merge(other.longTrends);
		this.shortTrends.merge(other.shortTrends);
	}
	
	@Override
	public String toString() {
		return String.format("%s\t\t%s\t\t%s", direction, longTrends, shortTrends);
	}
	
	public void print() {
		String format = "| %-7.7s | | %-7.7s | %-7.7s | %-7.7s | %-7.7s | %-7.7s | %-7.7s | | %-7.7s | %-7.7s | %-7.7s | %-7.7s | %-7.7s | %-7.7s |";
		System.out.println(" ---------   -----------------------------------------------------------   -----------------------------------------------------------");		
		System.out.println(format(format, 
				"direction", 
				
				"l_ema",
				"l_macdHistogram",
				"l_macdLinesSlope",
				"l_macdValue",
				"l_stocSlope",
				"l_stocValue",
				
				"s_ema",
				"s_macdHistogram",
				"s_macdLinesSlope",
				"s_macdValue",
				"s_stocSlope",
				"s_stocValue"
				));
		System.out.println("|---------| |---------|---------|---------|---------|---------|---------| |---------|---------|---------|---------|---------|---------|");		
		for(int i = 0; i < 3; i++) {
			System.out.println(format(format, 
					direction.asString(i), 
					
					longTrends.ema.asString(i),
					longTrends.macdHistogram.asString(i),
					longTrends.macdLinesSlope.asString(i),
					longTrends.macdValue.asString(i),
					longTrends.sstocSlope.asString(i),
					longTrends.sstocValue.asString(i),
					
					shortTrends.ema.asString(i),
					shortTrends.macdHistogram.asString(i),
					shortTrends.macdLinesSlope.asString(i),
					shortTrends.macdValue.asString(i),
					shortTrends.sstocSlope.asString(i),
					shortTrends.sstocValue.asString(i)
					));
		}
		System.out.println(" ---------   -----------------------------------------------------------   -----------------------------------------------------------");		
	}
	
	// ------------------------------------------------------------------------
	// inner classes
	// ------------------------------------------------------------------------

	@NoArgsConstructor
	@EqualsAndHashCode
	public static class DirectionPredicate implements Predicate<TradingDirection> {

		private List<TradingDirection> directions = new LinkedList<>();
		
		public DirectionPredicate(TradingDirection...direction) {
			this.directions = Arrays.asList(direction);
		}
		
		@Override
		public boolean test(TradingDirection t) {
			for(TradingDirection d : this.directions) {
				if( d == t ) {
					return true;
				}
			}
			
			return false;
		}
		
		public void merge(DirectionPredicate other) {
			other.directions.forEach(d -> {
				if(!this.directions.contains(d)) {
					this.directions.add(d);
				}
			});
			Collections.sort(directions);
		}
		
		public String asString(int index) {
			if(directions.size() <= index) {
				return "";
			} else {
				return directions.get(index).name();
			}
		}
		
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			directions.forEach(d -> sb.append(String.format("%.1s", d.name())));
			
			return sb.toString();
		}
		
	}
	
	@AllArgsConstructor
	@Builder
	@EqualsAndHashCode
	public static class TrendsPredicate implements Predicate<Trends> {

		private final TrendPredicate ema;
		private final TrendPredicate macdHistogram;
		private final TrendPredicate macdLinesSlope;
		private final TrendPredicate macdValue;
		private final TrendPredicate sstocSlope;
		private final TrendPredicate sstocValue;

		public TrendsPredicate() {
			this.ema = new TrendPredicate();
			this.macdHistogram = new TrendPredicate();
			this.macdLinesSlope = new TrendPredicate();
			this.macdValue = new TrendPredicate();
			this.sstocSlope = new TrendPredicate();
			this.sstocValue = new TrendPredicate();
		}
		
		@Override
		public boolean test(Trends t) {
			return 
					ema.test(t.getEma())
					&& macdHistogram.test(t.getMacdHistogram())
					&& macdLinesSlope.test(t.getMacdLinesSlope())
					&& macdValue.test(t.getMacdValue())
					&& sstocSlope.test(t.getSstocSlope())
					&& sstocValue.test(t.getSstocValue());
		}
	
		public void merge(TrendsPredicate other) {
			this.ema.merge(other.ema);
			this.macdHistogram.merge(other.macdHistogram);
			this.macdLinesSlope.merge(other.macdLinesSlope);
			this.macdValue.merge(other.macdValue);
			this.sstocSlope.merge(other.sstocSlope);
			this.sstocValue.merge(other.sstocValue);
		}
		
		public String toString() {
			
			return String.format("%s\t%s\t%s\t%s\t%s\t%s", 
					ema,
					macdHistogram,
					macdLinesSlope,
					macdValue,
					sstocSlope,
					sstocValue);
		}
		
		@NoArgsConstructor
		@EqualsAndHashCode
		public static class TrendPredicate implements Predicate<TradingDirection> {

			private List<TradingDirection> directions = new LinkedList<>();
			
			public TrendPredicate(TradingDirection...direction) {
				this.directions = Arrays.asList(direction);
			}
			
			public boolean test(String tradingDirection) {
				return test(TradingDirection.valueOf(tradingDirection));
			}
			
			@Override
			public boolean test(TradingDirection t) {
				for(TradingDirection direction : this.directions) {
					if (direction.equals(t)) {
						return true;
					}
				}
				
				return false;
			}
			
			public void merge(TrendPredicate other) {
				other.directions.forEach(d -> {
					if(!this.directions.contains(d)) {
						this.directions.add(d);
					}
				});
				Collections.sort(directions);
			}
			
			public String asString(int index) {
				if(directions.size() <= index) {
					return "";
				} else {
					return directions.get(index).name();
				}
			}
			
			public String toString() {
				final StringBuilder sb = new StringBuilder();
				directions.forEach(d -> sb.append(String.format("%.1s", d.name())));
				
				return sb.toString();
			}
			
		}
	}
}
