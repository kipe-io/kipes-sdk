package de.tradingpulse.stage.systems.service.processors;

import java.util.Optional;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import de.tradingpulse.common.stream.aggregates.IncrementalAggregate;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.aggregates.ImpulseAggregate;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.stage.systems.recordtypes.ImpulseSourceRecord;

public class IncrementalImpulseTransformer implements Transformer<SymbolTimestampKey, ImpulseSourceRecord, KeyValue<SymbolTimestampKey, ImpulseRecord>> {
	
	private final String storeName;
	private KeyValueStore<String, IncrementalAggregate<ImpulseAggregate>> state;
	
	IncrementalImpulseTransformer(final String storeName) {
		this.storeName = storeName;
	}
	
	@SuppressWarnings("unchecked")
	public void init(ProcessorContext context) {
		this.state = (KeyValueStore<String, IncrementalAggregate<ImpulseAggregate>>)context.getStateStore(this.storeName);
	}
	
	public KeyValue<SymbolTimestampKey, ImpulseRecord> transform(SymbolTimestampKey key, ImpulseSourceRecord value) {
		
		IncrementalAggregate<ImpulseAggregate> incrementalAggregate = Optional
				.ofNullable(this.state.get(key.getSymbol()))
				.orElseGet(IncrementalAggregate::new);
		
		ImpulseAggregate impulseAggregate = Optional
				.ofNullable(incrementalAggregate.getAggregate(value.getTimeRangeTimestamp()))
				.orElseGet(ImpulseAggregate::new);
		
		ImpulseRecord impulseRecord = impulseAggregate.aggregate(value.getEmaData(), value.getMacdHistogramData());
		incrementalAggregate.setAggregate(value.getTimeRangeTimestamp(), impulseAggregate);
		if(impulseRecord != null) {
			impulseRecord.setKey(value.getKey());
			impulseRecord.setTimeRange(value.getTimeRange());			
		}
		this.state.put(key.getSymbol(), incrementalAggregate);
		
		if(impulseRecord == null ) {
			return null;
		}
		
		return new KeyValue<>(key, impulseRecord); 
	}
	
	public void close() { 
		// nothing to do
	}
}
