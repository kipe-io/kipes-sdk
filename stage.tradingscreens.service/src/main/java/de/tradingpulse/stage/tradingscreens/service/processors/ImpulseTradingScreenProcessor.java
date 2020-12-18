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
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class ImpulseTradingScreenProcessor extends AbstractProcessorFactory {
		
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
		createImpulseTradingScreenStream(
				tradingScreensStreamsFacade.getImpulseTradingScreenStreamName(), 
				systemsStreamsFacade.getImpulseDailyStream(), 
				systemsStreamsFacade.getImpulseWeeklyStream());
	}

	private void createImpulseTradingScreenStream(
			String topicName,
			KStream<SymbolTimestampKey, ImpulseRecord> shortTimeRangeStream,
			KStream<SymbolTimestampKey, ImpulseRecord> longTimeRangeStream
	) throws InterruptedException, ExecutionException {
		
		// setup join parameters
		// TODO externalize retention period
		// IDEA: (via AbstractStreamFactory.topics.XXX.retentionMs)
		// the config depends on the overall retention period and the earliest
		// day we fetch at the iexcloud connector. 
		final Duration storeRetentionPeriod = Duration.ofMillis(this.retentionMs + 86400000L); // we add a day to have today access to the full retention time (record create ts is start of day)
		final Duration windowSize = Duration.ofSeconds(0);
		final boolean retainDuplicates = true; // topology creation will fail on false 

		// create dedup store
		final String dedupStoreName = getProcessorStoreTopicName(topicName+"-dedup");
		StoreBuilder<KeyValueStore<String,ImpulseTradingScreenRecord>> dedupStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(dedupStoreName),
						jsonSerdeRegistry.getSerde(String.class),
						jsonSerdeRegistry.getSerde(ImpulseTradingScreenRecord.class));
		builder.addStateStore(dedupStoreBuilder);
		
		// create topology
		KStream<SymbolTimestampKey, ImpulseTradingScreenRecord> stsrStream = 
				shortTimeRangeStream
		
				// join daily and weekly impulse
				.join(
						
						longTimeRangeStream,
						
						// join logic below delivers swing trading screen data
						(shortTimeRangeImpulseRecord, longTimeRangeImpulseRecord) -> ImpulseTradingScreenRecord.builder()
									.key(shortTimeRangeImpulseRecord.getKey())
									.timeRange(shortTimeRangeImpulseRecord.getTimeRange())
									.shortRangeImpulseRecord(shortTimeRangeImpulseRecord)
									.longRangeImpulseRecord(longTimeRangeImpulseRecord)
									.build(),
									
						// window size can be small as we know the data is at minimum at minute intervals
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
		.transform(() -> new Transformer<SymbolTimestampKey, ImpulseTradingScreenRecord, KeyValue<SymbolTimestampKey, ImpulseTradingScreenRecord>>() {

			private KeyValueStore<String, ImpulseTradingScreenRecord> state;
			
			@SuppressWarnings("unchecked")
			public void init(ProcessorContext context) {
				this.state = (KeyValueStore<String, ImpulseTradingScreenRecord>)context.getStateStore(dedupStoreName);
			}

			@Override
			public KeyValue<SymbolTimestampKey, ImpulseTradingScreenRecord> transform(SymbolTimestampKey key, ImpulseTradingScreenRecord value) {
				ImpulseTradingScreenRecord oldValue = this.state.get(key.getSymbol());
				
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
				jsonSerdeRegistry.getSerde(ImpulseTradingScreenRecord.class)));
		
	}
}
