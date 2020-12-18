package de.tradingpulse.streams.kafka.processors;

import java.util.function.BiFunction;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Kafka Transformer to semantically de-duplicate records. Clients do not 
 * instanciate this class directly.
 * 
 * TODO document the exact behavior
 * TODO add tests
 *
 * @param <K> the key type of the de-duplicated records
 * @param <V> the value type of the de-duplicated records
 * @param <SK> the store key type to register already emitted values
 */
class DedupTransformer<K,V, SK> implements Transformer<K,V, KeyValue<K,V>> {

	private final String stateStoreName;
	private final BiFunction<K, V, SK> storeKeyFunction;
	
	private KeyValueStore<SK,V> stateStore;
	
	DedupTransformer(String stateStoreName, BiFunction<K, V, SK> storeKeyFunction) {
		this.stateStoreName = stateStoreName;
		this.storeKeyFunction = storeKeyFunction;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void init(ProcessorContext context) {
		this.stateStore = (KeyValueStore<SK,V>)context.getStateStore(stateStoreName);
	}

	@Override
	public KeyValue<K,V> transform(K key, V value) {
		SK storeKey = this.storeKeyFunction.apply(key, value);
		if(storeKey == null) {
			return null;
		}
		
		V firstRecord = this.stateStore.get(storeKey);
		if(firstRecord == null) {
			this.stateStore.put(storeKey, value);	
			return new KeyValue<>(key, value);
		}
		
		return null;
	}

	@Override
	public void close() {
		// nothing to do
	}
}