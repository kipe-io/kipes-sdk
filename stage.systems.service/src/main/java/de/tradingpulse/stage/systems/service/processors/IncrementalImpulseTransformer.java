package de.tradingpulse.stage.systems.service.processors;

import java.util.Optional;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import de.tradingpulse.common.stream.aggregates.IncrementalAggregate;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.aggregates.ImpulseAggregate;
import de.tradingpulse.stage.systems.recordtypes.ImpulseData;
import de.tradingpulse.stage.systems.recordtypes.ImpulseSourceData;

public class IncrementalImpulseTransformer implements Transformer<SymbolTimestampKey, ImpulseSourceData, KeyValue<SymbolTimestampKey, ImpulseData>> {
	
	private final String storeName;
	private KeyValueStore<String, IncrementalAggregate<ImpulseAggregate>> state;
	
	IncrementalImpulseTransformer(final String storeName) {
		this.storeName = storeName;
	}
	
	@SuppressWarnings("unchecked")
	public void init(ProcessorContext context) {
		this.state = (KeyValueStore<String, IncrementalAggregate<ImpulseAggregate>>)context.getStateStore(this.storeName);
	}
	
	public KeyValue<SymbolTimestampKey, ImpulseData> transform(SymbolTimestampKey key, ImpulseSourceData value) {
		
		IncrementalAggregate<ImpulseAggregate> incrementalAggregate = Optional
				.ofNullable(this.state.get(key.getSymbol()))
				.orElseGet(IncrementalAggregate::new);
		
		ImpulseAggregate impulseAggregate = Optional
				.ofNullable(incrementalAggregate.getAggregate(key.getTimestamp()))
				.orElseGet(ImpulseAggregate::new);
		
		ImpulseData impulseData = impulseAggregate.aggregate(value.getEmaData(), value.getMacdHistogramData());
		incrementalAggregate.setAggregate(key.getTimestamp(), impulseAggregate);
		this.state.put(key.getSymbol(), incrementalAggregate);
		
		if(impulseData == null ) {
			return null;
		}
		
		impulseData.setKey(key);
		
		return new KeyValue<>(key, impulseData); 
	}
	
	public void close() { 
		// nothing to do
	}
}
