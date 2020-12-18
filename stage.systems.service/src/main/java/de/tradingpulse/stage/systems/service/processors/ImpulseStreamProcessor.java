package de.tradingpulse.stage.systems.service.processors;

import static de.tradingpulse.streams.kafka.factories.TopicNamesFactory.getProcessorStoreTopicName;

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

import de.tradingpulse.common.stream.aggregates.IncrementalAggregate;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.stage.systems.recordtypes.ImpulseSourceRecord;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;
import de.tradingpulse.stages.indicators.streams.IndicatorsStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class ImpulseStreamProcessor extends AbstractProcessorFactory {
	
	@Inject
	private IndicatorsStreamsFacade indicatorsStreamsFacade;
	
	@Inject
	private SystemsStreamsFacade systemsStreamsFacade;
	
	@Inject
	private ConfiguredStreamBuilder builder;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected void initProcessors() throws InterruptedException, ExecutionException {
		// impulse daily
		createImpulseStream(
				systemsStreamsFacade.getImpulseDailyStreamName(), 
				indicatorsStreamsFacade.getEma13DailyStream(), 
				indicatorsStreamsFacade.getMacd12269DailyStream());
		
		// impulse weekly incremental
		createImpulseStream(
				systemsStreamsFacade.getImpulseWeeklyStreamName(), 
				indicatorsStreamsFacade.getEma13WeeklyStream(), 
				indicatorsStreamsFacade.getMacd12269WeeklyStream());
	}
	
	private void createImpulseStream(
			final String topicName,
			final KStream<SymbolTimestampKey, DoubleRecord> emaStream,
			final KStream<SymbolTimestampKey, MACDHistogramRecord> macdStream) 
	{
		// setup join parameters
		// TODO externalize retention period
		// IDEA: (via AbstractStreamFactory.topics.XXX.retentionMs)
		// the config depends on the overall retention period and the earliest
		// day we fetch at the iexcloud connector. 
		final Duration storeRetentionPeriod = Duration.ofMillis(this.retentionMs + 86400000L); // we add a day to have today access to the full retention time (record create ts is start of day)
		final Duration windowSize = Duration.ofSeconds(0);
		final boolean retainDuplicates = true; // topology creation will fail on false 

		// create transformer store
		final String transformerStoreName = getProcessorStoreTopicName(topicName+"-transformer");
		@SuppressWarnings("rawtypes")
		StoreBuilder<KeyValueStore<String,IncrementalAggregate>> transformerStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(transformerStoreName),
						jsonSerdeRegistry.getSerde(String.class),
						jsonSerdeRegistry.getSerde(IncrementalAggregate.class));
		builder.addStateStore(transformerStoreBuilder);

		// create dedup store
		final String dedupStoreName = getProcessorStoreTopicName(topicName+"-dedup");
		StoreBuilder<KeyValueStore<SymbolTimestampKey,ImpulseRecord>> dedupStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(dedupStoreName),
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class),
						jsonSerdeRegistry.getSerde(ImpulseRecord.class));
		builder.addStateStore(dedupStoreBuilder);

		// create topology
		KStream<SymbolTimestampKey, ImpulseSourceRecord> isrStream = emaStream
		
		// join ema and macd to Impulse
		.join(
				macdStream,
				// join logic below
				(emaData, macdHistogramData) -> ImpulseSourceRecord.builder()
						.key(emaData.getKey())
						.timeRange(emaData.getTimeRange())
						.emaData(emaData)
						.macdHistogramData(macdHistogramData)
						.build(),
				// window size can be small as we know the data is at minimum at minute intervals
				JoinWindows
				.of(windowSize)
				.grace(storeRetentionPeriod),
				// configuration of the underlying window join stores for keeping the data
				StreamJoined.<SymbolTimestampKey, DoubleRecord, MACDHistogramRecord>with(
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
				.withValueSerde(jsonSerdeRegistry.getSerde(DoubleRecord.class))
				.withOtherValueSerde(jsonSerdeRegistry.getSerde(MACDHistogramRecord.class)));
		
		isrStream
		// calculate the impulse data
		.transform(() -> new ImpulseTransformer(transformerStoreName), transformerStoreName)
		// deduplicate per SymbolTimestampKey
		.transform(() -> new Transformer<SymbolTimestampKey, ImpulseRecord, KeyValue<SymbolTimestampKey, ImpulseRecord>>() {

			private KeyValueStore<SymbolTimestampKey, ImpulseRecord> state;
			
			@SuppressWarnings("unchecked")
			public void init(ProcessorContext context) {
				this.state = (KeyValueStore<SymbolTimestampKey, ImpulseRecord>)context.getStateStore(dedupStoreName);
			}

			@Override
			public KeyValue<SymbolTimestampKey, ImpulseRecord> transform(SymbolTimestampKey key, ImpulseRecord value) {
				ImpulseRecord lastImpulseData = this.state.get(key);
				this.state.put(key, value);
				
				return value.equals(lastImpulseData)? null : new KeyValue<>(key, value);
			}

			@Override
			public void close() {
				// nothing to do
			}
			
		}, dedupStoreName)
		.to(topicName, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(ImpulseRecord.class)));
	}
}
