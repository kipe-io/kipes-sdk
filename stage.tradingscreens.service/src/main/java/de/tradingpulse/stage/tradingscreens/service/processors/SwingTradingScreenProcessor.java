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

import de.tradingpulse.common.stream.data.ImpulseData;
import de.tradingpulse.common.stream.data.SymbolTimestampKey;
import de.tradingpulse.common.utils.TimeUtils;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stage.tradingscreens.data.SwingTradingScreenData;
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
				systemsStreamsFacade.getImpulseWeeklyIncrementalStream());
	}

	private void createSwingTradingScreenStream(
			String topicName,
			KStream<SymbolTimestampKey, ImpulseData> shortTimeRangeStream,
			KStream<SymbolTimestampKey, ImpulseData> longTimeRangeStream
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
		StoreBuilder<KeyValueStore<SymbolTimestampKey,SwingTradingScreenData>> dedupStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(dedupStoreName),
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class),
						jsonSerdeRegistry.getSerde(SwingTradingScreenData.class));
		builder.addStateStore(dedupStoreBuilder);
		
		// create topology
		shortTimeRangeStream
		
		// map daily impulse data onto weekly key
		.map((key, value) -> {
			SymbolTimestampKey weeklyKey = SymbolTimestampKey.builder()
					.timestamp(TimeUtils.getStartOfWeekTimestampUTC(key.getTimestamp()))
					.symbol(key.getSymbol())
					.build();
			
			return new KeyValue<>(weeklyKey, value);
		})
		// push to intermediate sink as the subsequent join would otherwise create a topic by itself
		.through(shortTimeRangeReKeyedTopicName, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(ImpulseData.class)))
		// join daily and weekly impulse
		.join(
				
				longTimeRangeStream,
				
				// join logic below delivers swing trading screen data
				(shortTimeRangeImpulseData, longTimeRangeImpulseData) -> SwingTradingScreenData.builder()
							.key(shortTimeRangeImpulseData.getKey())
							.shortRangeImpulseData(shortTimeRangeImpulseData)
							.longRangeImpulseData(longTimeRangeImpulseData)
							.build(),
							
				// window size can be small as we know the data is at minimum at minute intervals
				// TODO: verify storeRetentionPeriod is suitable
				// background: I assume we would need to have a retention period 
				// for as long as the long time range is  
				JoinWindows
				.of(windowSize)
				.grace(storeRetentionPeriod),
				// configuration of the underlying window join stores for keeping the data
				StreamJoined.<SymbolTimestampKey, ImpulseData, ImpulseData>with(
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
				.withValueSerde(jsonSerdeRegistry.getSerde(ImpulseData.class))
				.withOtherValueSerde(jsonSerdeRegistry.getSerde(ImpulseData.class)))
		// deduplicate by both impulse data ignoring timestamps
		.transform(() -> new Transformer<SymbolTimestampKey, SwingTradingScreenData, KeyValue<SymbolTimestampKey, SwingTradingScreenData>>() {

			private KeyValueStore<SymbolTimestampKey, SwingTradingScreenData> state;
			
			@SuppressWarnings("unchecked")
			public void init(ProcessorContext context) {
				this.state = (KeyValueStore<SymbolTimestampKey, SwingTradingScreenData>)context.getStateStore(dedupStoreName);
			}

			@Override
			public KeyValue<SymbolTimestampKey, SwingTradingScreenData> transform(SymbolTimestampKey key, SwingTradingScreenData value) {
				SwingTradingScreenData oldValue = this.state.get(key);
				this.state.put(key, value);
				
				if(oldValue == null) {
					return new KeyValue<>(key, value);
				}
				
				ImpulseData oldLTImpulse = oldValue.getLongRangeImpulseData();
				ImpulseData newLTImpulse = value.getLongRangeImpulseData();
				boolean sameLongTimeRangeImpulse = oldLTImpulse.isSameImpulse(newLTImpulse);
				
				ImpulseData oldSTImpulse = oldValue.getShortRangeImpulseData();
				ImpulseData newSTImpulse = value.getShortRangeImpulseData();
				boolean sameShortTimeRangeImpulse = oldSTImpulse.isSameImpulse(newSTImpulse);
				
				return sameLongTimeRangeImpulse && sameShortTimeRangeImpulse ? null : new KeyValue<>(key, value);
			}

			@Override
			public void close() {
				// nothing to do
			}
			
		}, dedupStoreName)
		
		.to(topicName, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(SwingTradingScreenData.class)));
		
	}
}
