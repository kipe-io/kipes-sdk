package de.tradingpulse.stage.indicators.service.processors;

import java.util.Optional;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import de.tradingpulse.common.stream.aggregates.IncrementalAggregate;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stages.indicators.aggregates.MACDHistogramAggregate;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;

class IncrementalMACDTransformer implements Transformer<SymbolTimestampKey, OHLCVRecord, KeyValue<SymbolTimestampKey, MACDHistogramRecord>> {
	
	private final String storeName;
	private final int fastPeriod;
	private final int slowPeriod;
	private final int signalPeriod;
	private KeyValueStore<String, IncrementalAggregate<MACDHistogramAggregate>> state;
	
	IncrementalMACDTransformer(
			final String storeName,
			final int fastPeriod,
			final int slowPeriod,
			final int signalPeriod)
	{
		this.storeName = storeName;
		this.fastPeriod = fastPeriod;
		this.slowPeriod = slowPeriod;
		this.signalPeriod = signalPeriod;
	}
	
	@SuppressWarnings("unchecked")
	public void init(ProcessorContext context) {
		this.state = (KeyValueStore<String, IncrementalAggregate<MACDHistogramAggregate>>)context.getStateStore(this.storeName);
	}
	
	public KeyValue<SymbolTimestampKey, MACDHistogramRecord> transform(SymbolTimestampKey key, OHLCVRecord value) {
		
		IncrementalAggregate<MACDHistogramAggregate> incrementalAggregate = Optional
				.ofNullable(this.state.get(key.getSymbol()))
				.orElseGet(IncrementalAggregate::new);
		
		MACDHistogramAggregate macdAggregate = Optional
				.ofNullable(incrementalAggregate.getAggregate(value.getTimeRangeTimestamp()))
				.orElseGet(() -> new MACDHistogramAggregate(this.fastPeriod, this.slowPeriod, this.signalPeriod));
		
		MACDHistogramRecord macdHistogramRecord = macdAggregate.aggregate(value.getClose());

		incrementalAggregate.setAggregate(value.getTimeRangeTimestamp(), macdAggregate);
		this.state.put(key.getSymbol(), incrementalAggregate);
		
		if(macdHistogramRecord == null) {
			return null;
		}
		
		macdHistogramRecord.setKey(value.getKey());
		macdHistogramRecord.setTimeRange(value.getTimeRange());
		
		return new KeyValue<>(key, macdHistogramRecord); 
	}
	
	public void close() { 
		// nothing to do
	}
}