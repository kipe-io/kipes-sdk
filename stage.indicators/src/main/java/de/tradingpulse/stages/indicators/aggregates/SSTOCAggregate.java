package de.tradingpulse.stages.indicators.aggregates;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import com.fasterxml.jackson.annotation.JsonProperty;

import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stages.indicators.recordtypes.SSTOCRecord;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SSTOCAggregate implements OHLCVRecordAggregate<SSTOCRecord, SSTOCAggregate>{

	private int n;
	@JsonProperty
	private ArrayBlockingQueue<OHLCVRecord> inputQueue;
	
	private int p1;
	@JsonProperty
	private ArrayBlockingQueue<Double> rawStochasticQueue = null;
	
	private int p2;
	@JsonProperty
	private ArrayBlockingQueue<Double> fastStochasticQueue = null;
	
	private Double fastStochastic = null;
	private Double slowStochastic = null;
	
	/**
	 * @param n
	 * 	period of days to calculate the raw stochastic
	 * 
	 * @param p
	 *  sma period to calculate fast and slow stochastic
	 */
	public SSTOCAggregate(int n, int p1, int p2) {
		this.n = n;
		this.p1 = p1;
		this.p2 = p2;
	}
	
	@Override
	public SSTOCAggregate deepClone() {
		SSTOCAggregate clone = new SSTOCAggregate();
		clone.n = this.n;
	
		Queue<OHLCVRecord> cloneInputQueue = clone.getInputQueue();
		this.getInputQueue().forEach(record -> cloneInputQueue.add(record.deepClone()));
	
		clone.p1 = this.p1;
		clone.getRawStochasticQueue().addAll(this.getRawStochasticQueue());
		clone.p2 = this.p2;
		clone.getFastStochasticQueue().addAll(this.getFastStochasticQueue());
		clone.fastStochastic = this.fastStochastic;
		clone.slowStochastic = this.slowStochastic;
		
		return clone;
	}

	/**
	 * Calculates the Slow Stochastic of a given OHLCVRecord sequence. The
	 * returned SSTOCRecord will be null for the first n + 2*p - 3 records.
	 */
	public SSTOCRecord aggregate(OHLCVRecord record) {
		
		Double oldFastStochastic = this.fastStochastic;
		Double oldSlowStochastic = this.slowStochastic;
		
		updateInputQueue(record);
		
		if(getInputQueue().size() < n) {
			return null;
		}
		
		// step 1: calculate %K, raw stochastic
		double highestHigh = calcHighestHigh();
		double lowestLow = calcLowestLow();
		double rawStochastic = highestHigh - lowestLow == 0? 
				0 : ((record.getClose() - lowestLow) / (highestHigh - lowestLow)) * 100.0;
		updateRawStochasticQueue(rawStochastic);
		
		if(getRawStochasticQueue().size() < this.p1) {
			return null;
		}
		
		// step 2: calculate %D, smoothed stochastic
		// for the slow stochastic %D is the fast stochastic line 
		this.fastStochastic = calcFastStochastic();
		updateFastStochasticQueue(fastStochastic);
		
		if(getFastStochasticQueue().size() < this.p2) {
			return null;
		}
		
		// step 3: calculate slow stochastic
		this.slowStochastic = calcSlowStochastic();
		
		return SSTOCRecord.builder()
				.key(record.getKey())
				.timeRange(record.getTimeRange())
				.fast(this.fastStochastic)
				.fChange(oldFastStochastic == null? null : oldFastStochastic.doubleValue() - this.fastStochastic.doubleValue())
				.slow(this.slowStochastic)
				.sChange(oldSlowStochastic == null? null : oldSlowStochastic.doubleValue() - this.slowStochastic.doubleValue())
				.build();
	}

	// ------------------------------------------------------------------------
	// input queue
	// ------------------------------------------------------------------------

	synchronized Queue<OHLCVRecord> getInputQueue() {
		if(this.inputQueue == null) {
			this.inputQueue = new ArrayBlockingQueue<>(n);
		}
		
		return this.inputQueue;
	}
	
	void updateInputQueue(OHLCVRecord record) {
		Queue<OHLCVRecord> recordsQueue = getInputQueue();
		if(recordsQueue.size() == this.n) {
			recordsQueue.poll();
		}
		recordsQueue.add(record);
	}
	
	double calcLowestLow() {
		Queue<OHLCVRecord> recordsQueue = getInputQueue();
		if(recordsQueue.size() < this.n) {
			throw new IllegalStateException("not enough OHLCVRecords aggregated yet");
		}
		
		return recordsQueue.stream()
				.min((OHLCVRecord o1, OHLCVRecord o2) -> o1.getLow().compareTo(o2.getLow()))
				.orElseThrow()
				.getLow();
	}
	
	double calcHighestHigh() {
		Queue<OHLCVRecord> recordsQueue = getInputQueue();
		if(recordsQueue.size() < this.n) {
			throw new IllegalStateException("not enough OHLCVRecords aggregated yet");
		}
		
		return recordsQueue.stream()
				.max((OHLCVRecord o1, OHLCVRecord o2) -> o1.getHigh().compareTo(o2.getHigh()))
				.orElseThrow()
				.getHigh();
	}
	
	// ------------------------------------------------------------------------
	// raw stochastic queue
	// ------------------------------------------------------------------------

	synchronized Queue<Double> getRawStochasticQueue() {
		if(this.rawStochasticQueue == null) {
			this.rawStochasticQueue = new ArrayBlockingQueue<>(this.p1);
		}
		
		return this.rawStochasticQueue;
	}
	
	void updateRawStochasticQueue(double rawStochastic) {
		Queue<Double> rawQueue = getRawStochasticQueue();
		if(rawQueue.size() == this.p1) {
			rawQueue.poll();
		}
		rawQueue.add(rawStochastic);
	}
	
	
	double calcFastStochastic() {
		Queue<Double> rawQueue = getRawStochasticQueue();
		if(rawQueue.size() < this.p1) {
			throw new IllegalStateException("not enough raw stochastic values aggregated yet");
		}
		
		return rawQueue.stream()
				.reduce(0.0, (aggregate, raw) -> aggregate + raw) / this.p1;
	}
	
	// ------------------------------------------------------------------------
	// fast stochastic queue
	// ------------------------------------------------------------------------

	synchronized Queue<Double> getFastStochasticQueue() {
		if(this.fastStochasticQueue == null) {
			this.fastStochasticQueue = new ArrayBlockingQueue<>(this.p2);
		}
		
		return this.fastStochasticQueue;
	}
	
	void updateFastStochasticQueue(double fastStochastic) {
		Queue<Double> fastQueue = getFastStochasticQueue();
		if(fastQueue.size() == this.p2) {
			fastQueue.poll();
		}
		fastQueue.add(fastStochastic);
	}
	
	double calcSlowStochastic() {
		Queue<Double> fastQueue = getFastStochasticQueue();
		if(fastQueue.size() < this.p2) {
			throw new IllegalStateException("not enough fast stochastic values aggregated yet");
		}
		
		return fastQueue.stream()
				.reduce(0.0, (aggregate, fast) -> aggregate + fast) / this.p2;
	}
}
