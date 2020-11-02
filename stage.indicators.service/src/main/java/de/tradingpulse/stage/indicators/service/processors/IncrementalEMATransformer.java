package de.tradingpulse.stage.indicators.service.processors;

import java.util.Optional;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tradingpulse.common.stream.aggregates.IncrementalAggregate;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stages.indicators.aggregates.EMAAggregate;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;

class IncrementalEMATransformer implements Transformer<SymbolTimestampKey, OHLCVRecord, KeyValue<SymbolTimestampKey, DoubleRecord>> {
	
	private static final Logger LOG = LoggerFactory.getLogger(IncrementalEMATransformer.class);
	
	private final String storeName;
	private final int numObservations;
	private KeyValueStore<String, IncrementalAggregate<EMAAggregate>> state;
	
	IncrementalEMATransformer(
			final String storeName,
			final int numObservations)
	{
		this.storeName = storeName;
		this.numObservations = numObservations;
	}
	
	@SuppressWarnings("unchecked")
	public void init(ProcessorContext context) {
		this.state = (KeyValueStore<String, IncrementalAggregate<EMAAggregate>>)context.getStateStore(this.storeName);
	}
	
	public KeyValue<SymbolTimestampKey, DoubleRecord> transform(SymbolTimestampKey key, OHLCVRecord value) {
		
		IncrementalAggregate<EMAAggregate> incrementalAggregate = Optional
				.ofNullable(this.state.get(key.getSymbol()))
				.orElseGet(IncrementalAggregate::new);
		
		EMAAggregate emaAggregate = Optional
				.ofNullable(incrementalAggregate.getAggregate(key.getTimestamp()))
				.orElseGet(() -> new EMAAggregate(this.numObservations));
		
		DoubleRecord emaData = emaAggregate.aggregate(value.getClose());
		LOG.debug("transform@{}: {}, {} -> {}", this.storeName, key, value, emaData);

		boolean stored = incrementalAggregate.setAggregate(key.getTimestamp(), emaAggregate);
		
		if(! stored ) {
			LOG.warn("transform@{}: out of order {}, must not be earlier than {}. Value ignored.", this.storeName, value, incrementalAggregate.getStableTimestamp());
		}
		
		this.state.put(key.getSymbol(), incrementalAggregate);
		
		if(emaData == null) {
			return null;
		}
		
		emaData.setKey(value.getKey());
		emaData.setTimeRange(value.getTimeRange());
		
		return new KeyValue<>(key, emaData); 
	}
	
	public void close() { 
		// nothing to do
	}
}