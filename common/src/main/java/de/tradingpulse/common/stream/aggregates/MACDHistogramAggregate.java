package de.tradingpulse.common.stream.aggregates;

import de.tradingpulse.common.stream.recordtypes.DoubleData;
import de.tradingpulse.common.stream.recordtypes.MACDHistogramData;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Aggregate to calculate the MACDHistogram(fast,slow,signal) of a stream of double values.
 */
@Data
@NoArgsConstructor
public class MACDHistogramAggregate implements DeepCloneable<MACDHistogramAggregate>{

	private int fastPeriod;
	private int slowPeriod;
	private int signalPeriod;
	
	private EMAAggregate emaFastAggregate = null;
	private EMAAggregate emaSlowAggregate = null;
	private EMAAggregate emaSignalAggregate = null;

	private Double macd;
	private Double signal;
	private Double histogram;
	
	public MACDHistogramAggregate(int fastPeriod, int slowPeriod, int signalPeriod) {
		this.fastPeriod = fastPeriod;
		this.slowPeriod = slowPeriod;
		this.signalPeriod = signalPeriod;
	}
	
	public MACDHistogramAggregate deepClone() {
		MACDHistogramAggregate clone = new MACDHistogramAggregate(this.fastPeriod, this.slowPeriod, this.signalPeriod);
		
		clone.emaFastAggregate = this.emaFastAggregate == null ? null : this.emaFastAggregate.deepClone();
		clone.emaSlowAggregate = this.emaSlowAggregate == null ? null : this.emaSlowAggregate.deepClone();
		clone.emaSignalAggregate = this.emaSignalAggregate == null ? null : this.emaSignalAggregate.deepClone();
		
		clone.macd = this.macd;
		clone.signal = this.signal;
		clone.histogram = this.histogram;
		
		return clone;
	}
	
	private synchronized DoubleData getEMAFast(double value) {
		if(this.emaFastAggregate == null) {
			this.emaFastAggregate = new EMAAggregate(fastPeriod);	
		}
		
		return this.emaFastAggregate.aggregate(value);
	}
	
	private synchronized DoubleData getEMASlow(double value) {
		if(this.emaSlowAggregate == null) {
			this.emaSlowAggregate = new EMAAggregate(slowPeriod);	
		}
		
		return this.emaSlowAggregate.aggregate(value);
	}
	
	private synchronized DoubleData getEMASignal(double macd) {
		if(this.emaSignalAggregate == null) {
			this.emaSignalAggregate = new EMAAggregate(signalPeriod);	
		}
		
		return this.emaSignalAggregate.aggregate(macd);
	}
	
	/**
	 * Aggregates a new value to calculate the current MACD histogram.
	 * Please note the returned {@link MACDHistogramData} has no key set and
	 * might be null for early values in the stream (for exactly slow + signal - 2 values). 
	 */
	public MACDHistogramData aggregate(double value) {
		
		Double mOld = this.macd;
		Double sOld = this.signal;
		Double hOld = this.histogram;
		
		// we need to aggregate both to get the EMAs loaded
		DoubleData emaFast = getEMAFast(value);
		DoubleData emaSlow = getEMASlow(value);
		if(emaFast == null || emaSlow == null) {
			return null;
		}
		
		this.macd = emaFast.getValue() - emaSlow.getValue();
		DoubleData emaSignal = getEMASignal(macd);
		if(emaSignal == null) {
			return null;
		}
		
		this.signal = emaSignal.getValue();
		this.histogram = this.macd - this.signal;
		
		return MACDHistogramData.builder()
				.macd(this.macd)
				.mChange(mOld == null? null : this.macd - mOld)
				.signal(this.signal)
				.sChange(sOld == null? null : this.signal - sOld)
				.histogram(this.histogram)
				.hChange(hOld == null? null : this.histogram - hOld)
				.build();
	}
}
