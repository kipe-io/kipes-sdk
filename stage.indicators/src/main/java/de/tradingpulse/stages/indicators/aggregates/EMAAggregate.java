package de.tradingpulse.stages.indicators.aggregates;

import com.fasterxml.jackson.annotation.JsonIgnore;

import de.tradingpulse.common.stream.aggregates.DeepCloneable;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Aggregate to calculate the EMA(n) of a stream of double values.
 * 
 * Initial value is calculated as SMA(n).
 */
@Data
@NoArgsConstructor
public class EMAAggregate implements DeepCloneable<EMAAggregate>{
	
	private int n;
	
	@JsonIgnore
	@Setter(AccessLevel.NONE)
	private Double k;
	
	private int count = 0;
	private Double initialSma = 0.0;
	
	private Double ema = null;
	
	public EMAAggregate(int n) {
		this.n = n;
	}
	
	@Override
	public EMAAggregate deepClone() {
		EMAAggregate clone = new EMAAggregate();
		clone.n = this.n;
		clone.k = this.k;
		clone.count = this.count;
		clone.initialSma = this.initialSma;
		clone.ema = this.ema;
		
		return clone;
	}
	
	private synchronized double getK() {
		if(this.k == null) {
			this.k = 2.0 / ( n + 1.0 );					
		}
		
		return this.k;
	}
	
	/**
	 * Aggregates a new value.
	 * Please note the returned {@link DoubleRecord} has no key set and might be
	 * null for early values in the stream (for exactly n - 1 values). 
	 */
	public DoubleRecord aggregate(double value) {
		
		Double vOld = this.ema;
		
		this.count++;
		
		if (this.count <= n) {
			
			// see https://nestedsoftware.com/2018/03/20/calculating-a-moving-average-on-streaming-data-5a7k.22879.html
			
			final double differential = (value - this.initialSma) / this.count; 
			
			this.initialSma += differential;
			
			if (this.count < this.n) {
				return null;
			}
			
			this.ema = initialSma;
		
		} else {
			final double factor = getK();
			
			this.ema = value * factor + this.ema * (1.0-factor);
		}
		
		return DoubleRecord.builder()
				.value(this.ema)
				.vChange(vOld == null? null : this.ema - vOld)
				.build();
	}
}