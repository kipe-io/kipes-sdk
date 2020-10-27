package de.tradingpulse.stage.systems.service.processors;

import java.time.Duration;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import de.tradingpulse.common.stream.aggregates.IncrementalAggregate;
import de.tradingpulse.common.stream.data.DoubleData;
import de.tradingpulse.common.stream.data.ImpulseData;
import de.tradingpulse.common.stream.data.ImpulseSourceData;
import de.tradingpulse.common.stream.data.MACDHistogramData;
import de.tradingpulse.common.stream.data.SymbolTimestampKey;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stages.indicators.streams.IndicatorsStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class ImpulseIncrementalStreamProcessor extends AbstractProcessorFactory {
	
	@Inject
	private IndicatorsStreamsFacade indicatorsStreamsFacade;
	
	@Inject
	private SystemsStreamsFacade systemsStreamsFacade;
	
	@Inject
	private ConfiguredStreamBuilder builder;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected void initProcessors() {
		// impulse daily
		createImpulseStream(
				systemsStreamsFacade.getImpulseDailyStreamName(), 
				indicatorsStreamsFacade.getEma13DailyStream(), 
				indicatorsStreamsFacade.getMacd12269DailyStream());
		
		// impulse weekly incremental
		createImpulseStream(
				systemsStreamsFacade.getImpulseWeeklyIncrementalStreamName(), 
				indicatorsStreamsFacade.getEma13WeeklyIncrementalStream(), 
				indicatorsStreamsFacade.getMacd12269WeeklyIncrementalStream());
	}
	
	private void createImpulseStream(
			final String topicName,
			final KStream<SymbolTimestampKey, DoubleData> emaStream,
			final KStream<SymbolTimestampKey, MACDHistogramData> macdStream 
	) {

		// setup join parameters
		final Duration storeRetentionPeriod = Duration.ofMinutes(10);
		final Duration windowSize = Duration.ofSeconds(0);
		final boolean retainDuplicates = true; // topology creation will fail on false 

		// create processor store
		final String storeName = getProcessorStoreTopicName(topicName);
		@SuppressWarnings("rawtypes")
		StoreBuilder<KeyValueStore<String,IncrementalAggregate>> keyValueStoreBuilder =
				Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName),
						jsonSerdeRegistry.getSerde(String.class),
						jsonSerdeRegistry.getSerde(IncrementalAggregate.class));
		builder.addStateStore(keyValueStoreBuilder);

		// create topology
		emaStream
		
		// join ema and macd to Impulse
		.join(
				macdStream,
				// join logic below
				(emaData, macdHistogramData) -> new ImpulseSourceData(
						emaData.getKey(),
						emaData,
						macdHistogramData),
				// window size can be small as we know the data is at minimum at minute intervals
				JoinWindows
				.of(windowSize)
				.grace(storeRetentionPeriod),
				// configuration of the underlying window join stores for keeping the data
				StreamJoined.<SymbolTimestampKey, DoubleData, MACDHistogramData>with(
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
				.withValueSerde(jsonSerdeRegistry.getSerde(DoubleData.class))
				.withOtherValueSerde(jsonSerdeRegistry.getSerde(MACDHistogramData.class)))
		
		// calculate the impulse data
		.transform(() -> new IncrementalImpulseTransformer(storeName), storeName)
		.to(topicName, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(ImpulseData.class)));
	}
}
