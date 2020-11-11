package de.tradingpulse.stage.tradingscreens.service.processors;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stage.tradingscreens.data.SwingTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class SwingTradingScreenProcessor extends AbstractProcessorFactory {
		
	@Inject
	private SystemsStreamsFacade systemsStreamsFacade;
	
	@Inject
	private TradingScreensStreamsFacade tradingScreensStreamsFacade;
	
	@Inject
	private ConfiguredStreamBuilder builder;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() throws Exception {
		createSwingTradingScreenStream(
				tradingScreensStreamsFacade.getSwingTradingScreenStreamName(), 
				systemsStreamsFacade.getImpulseDailyStream(), 
				systemsStreamsFacade.getImpulseWeeklyStream());
	}

	private void createSwingTradingScreenStream(
			String topicName,
			KStream<SymbolTimestampKey, ImpulseRecord> shortTimeRangeStream,
			KStream<SymbolTimestampKey, ImpulseRecord> longTimeRangeStream
	) throws InterruptedException, ExecutionException {
		
		// setup shortTimeRange re-keyed topic
		String shortTimeRangeReKeyedTopicName = topicName + "-shortterm-impulse-rekeyed";
		ensureTopics(shortTimeRangeReKeyedTopicName);
		
		// setup join parameters
		final Duration storeRetentionPeriod = Duration.ofMinutes(10);
		final Duration windowSize = Duration.ofSeconds(0);
		final boolean retainDuplicates = true; // topology creation will fail on false 

		// create dedup store
		final String dedupStoreName = getProcessorStoreTopicName(topicName+"-dedup");
		StoreBuilder<KeyValueStore<String,SwingTradingScreenRecord>> dedupStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(dedupStoreName),
						jsonSerdeRegistry.getSerde(String.class),
						jsonSerdeRegistry.getSerde(SwingTradingScreenRecord.class));
		builder.addStateStore(dedupStoreBuilder);
		
		// create topology
		KStream<SymbolTimestampKey, SwingTradingScreenRecord> stsrStream = 
				shortTimeRangeStream
		
				// join daily and weekly impulse
				.join(
						
						longTimeRangeStream,
						
						// join logic below delivers swing trading screen data
						(shortTimeRangeImpulseRecord, longTimeRangeImpulseRecord) -> SwingTradingScreenRecord.builder()
									.key(shortTimeRangeImpulseRecord.getKey())
									.timeRange(shortTimeRangeImpulseRecord.getTimeRange())
									.shortRangeImpulseRecord(shortTimeRangeImpulseRecord)
									.longRangeImpulseRecord(longTimeRangeImpulseRecord)
									.build(),
									
						// window size can be small as we know the data is at minimum at minute intervals
						// TODO: verify storeRetentionPeriod is suitable
						// background: I assume we would need to have a retention period 
						// for as long as the long time range is 
						JoinWindows
						.of(windowSize)
						.grace(storeRetentionPeriod),
						// configuration of the underlying window join stores for keeping the data
						StreamJoined.<SymbolTimestampKey, ImpulseRecord, ImpulseRecord>with(
								Stores.persistentWindowStore(
										topicName+"-join-store-left", 
										storeRetentionPeriod, 
										windowSize, 
										retainDuplicates), 
								Stores.persistentWindowStore(
										topicName+"-join-store-right", 
										storeRetentionPeriod, 
										windowSize, 
										retainDuplicates))
						.withKeySerde(jsonSerdeRegistry.getSerde(SymbolTimestampKey.class))
						.withValueSerde(jsonSerdeRegistry.getSerde(ImpulseRecord.class))
						.withOtherValueSerde(jsonSerdeRegistry.getSerde(ImpulseRecord.class)));
		
		stsrStream
		// deduplicate by both impulse data ignoring timestamps
		.transform(() -> new Transformer<SymbolTimestampKey, SwingTradingScreenRecord, KeyValue<SymbolTimestampKey, SwingTradingScreenRecord>>() {

			private KeyValueStore<String, SwingTradingScreenRecord> state;
			
			@SuppressWarnings("unchecked")
			public void init(ProcessorContext context) {
				this.state = (KeyValueStore<String, SwingTradingScreenRecord>)context.getStateStore(dedupStoreName);
			}

			@Override
			public KeyValue<SymbolTimestampKey, SwingTradingScreenRecord> transform(SymbolTimestampKey key, SwingTradingScreenRecord value) {
				SwingTradingScreenRecord oldValue = this.state.get(key.getSymbol());
				
				if(oldValue == null) {
					this.state.put(key.getSymbol(), value);
					return new KeyValue<>(key, value);
				}
				
				ImpulseRecord oldLTImpulse = oldValue.getLongRangeImpulseRecord();
				ImpulseRecord newLTImpulse = value.getLongRangeImpulseRecord();
				boolean sameLongTimeRangeImpulse = oldLTImpulse.isSameImpulse(newLTImpulse);
				
				ImpulseRecord oldSTImpulse = oldValue.getShortRangeImpulseRecord();
				ImpulseRecord newSTImpulse = value.getShortRangeImpulseRecord();
				boolean sameShortTimeRangeImpulse = oldSTImpulse.isSameImpulse(newSTImpulse);
				
				if(sameLongTimeRangeImpulse && sameShortTimeRangeImpulse) {
					return null;
				}
				
				this.state.put(key.getSymbol(), value);
				
				return new KeyValue<>(key, value);
			}

			@Override
			public void close() {
				// nothing to do
			}
			
		}, dedupStoreName)
		
		.to(topicName, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(SwingTradingScreenRecord.class)));
		
	}
}
