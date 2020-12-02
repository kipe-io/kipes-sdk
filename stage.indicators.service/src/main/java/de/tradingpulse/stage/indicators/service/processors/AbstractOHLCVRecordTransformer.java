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
import de.tradingpulse.stages.indicators.aggregates.OHLCVRecordAggregate;

public abstract class AbstractOHLCVRecordTransformer <A extends OHLCVRecordAggregate<T, A>, T>
implements Transformer<SymbolTimestampKey, OHLCVRecord, KeyValue<SymbolTimestampKey, T>> 
{
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractOHLCVRecordTransformer.class);
	
	private final String storeName;
	private KeyValueStore<String, IncrementalAggregate<A>> state;
	
	protected AbstractOHLCVRecordTransformer(String storeName) {
		this.storeName = storeName;
	}
	
	@SuppressWarnings("unchecked")
	public void init(ProcessorContext context) {
		this.state = (KeyValueStore<String, IncrementalAggregate<A>>)context.getStateStore(this.storeName);
	}
	
	protected KeyValueStore<String, IncrementalAggregate<A>> getStateStore() {
		// TODO check initialized
		return this.state;
	}
	
	protected abstract A getInitialAggregate();
	
	public KeyValue<SymbolTimestampKey, T> transform(SymbolTimestampKey key, OHLCVRecord value) {
		
		IncrementalAggregate<A> incrementalAggregate = Optional
				.ofNullable(getStateStore().get(key.getSymbol()))
				.orElseGet(IncrementalAggregate::new);
		
		A aggregate = Optional
				.ofNullable(incrementalAggregate.getAggregate(value.getTimeRangeTimestamp()))
				.orElseGet(this::getInitialAggregate);
		
		T aggregatedValue = aggregate.aggregate(value);

		boolean stored = incrementalAggregate.setAggregate(value.getTimeRangeTimestamp(), aggregate);
		
		if(! stored ) {
			LOG.warn("transform@{}: out of order {}, must not be earlier than {}. Value ignored.", this.storeName, value, incrementalAggregate.getStableTimestamp());
		}
		
		this.state.put(key.getSymbol(), incrementalAggregate);
		
		if(aggregatedValue == null) {
			return null;
		}
		
		return new KeyValue<>(key, aggregatedValue); 
	}
	
	public void close() { 
		// nothing to do
	}
}
