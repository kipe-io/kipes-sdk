package de.tradingpulse.timelinetest.streams;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import de.tradingpulse.common.stream.data.OHLCVData;
import de.tradingpulse.common.stream.data.SymbolTimestampKey;
import de.tradingpulse.common.utils.TimeUtils;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
public class MinuteStreamFactory extends AbstractStreamFactory {
	public static final String TOPIC_DATA_15SECS_GROUPED_MINUTELY = "timelinetest-data_15_sec_grouped_minutely";
	public static final String TOPIC_DATA_AGG_MINUTE = "timelinetest-data_agg_minute";
	private static final String STORE_MINUTE_AGGREGATE = "data_agg_minute-store";
	
	public static final String TOPIC_DATA_15SECS_GROUPED_SYMBOL = "timelinetest-data_15_sec_grouped_symbol";
	public static final String TOPIC_DATA_WINDOWED_MINUTE = "timelinetest-data_win_minute";
	private static final String STORE_SYMBOL_AGGREGATE = "data_agg_symbol-store";
	
	@Inject
	private JsonSerdeRegistry serdeRegistry;
	
	@Inject @Named(SecondsStreamsFactory.TOPIC_DATA_15_SEC)
	private KStream<SymbolTimestampKey, OHLCVData> data15Sec;
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
			TOPIC_DATA_15SECS_GROUPED_MINUTELY,
			TOPIC_DATA_AGG_MINUTE,
			TOPIC_DATA_15SECS_GROUPED_SYMBOL,
			TOPIC_DATA_WINDOWED_MINUTE
		};
	}
	
	@Override
	protected Set<String> getTopicNamesForDeletion() {
		Set<String> rawTopicNames = super.getTopicNamesForDeletion();
		rawTopicNames.add("timelinetest-"+TOPIC_DATA_15SECS_GROUPED_MINUTELY+"-changelog");
		rawTopicNames.add("timelinetest-"+TOPIC_DATA_15SECS_GROUPED_SYMBOL+"-changelog");
		
		return rawTopicNames;
	}
	
	@Singleton @Named(TOPIC_DATA_AGG_MINUTE)
	KStream<SymbolTimestampKey, OHLCVData> dataAggMinuteStream(final ConfiguredStreamBuilder builder)
	throws InterruptedException, ExecutionException
	{
		data15Sec.map((data15Key, data15Value) -> {
			
			SymbolTimestampKey minuteKey = SymbolTimestampKey.builder()
					.symbol(data15Key.getSymbol())
					.timestamp(TimeUtils
							.getStartOfMinuteTimestampUTC(data15Key.getTimestamp()))
					.build();
			
			return new KeyValue<>(minuteKey, data15Value);
		})
		.through(TOPIC_DATA_15SECS_GROUPED_MINUTELY, Produced.with(
				serdeRegistry.getSerde(SymbolTimestampKey.class), 
				serdeRegistry.getSerde(OHLCVData.class)))
		.groupByKey()
		.<OHLCVData>aggregate(() -> null, (key, ohlcvData, aggregateOhlcvData ) -> {
			
			if(aggregateOhlcvData == null) {
				// the data gets the same key as the group (essentially setting the timestamp to week start) 
				ohlcvData.setKey(key);
				return ohlcvData;
			}
			return aggregateOhlcvData.aggregateWith(ohlcvData);
		}, Materialized
				.<SymbolTimestampKey, OHLCVData, KeyValueStore<Bytes,byte[]>>as(STORE_MINUTE_AGGREGATE)
				.withKeySerde(serdeRegistry.getSerde(SymbolTimestampKey.class))
				.withValueSerde(serdeRegistry.getSerde(OHLCVData.class))
				.withCachingDisabled())
		.toStream()
		.to(TOPIC_DATA_AGG_MINUTE, Produced.with(
				serdeRegistry.getSerde(SymbolTimestampKey.class), 
				serdeRegistry.getSerde(OHLCVData.class)));
		
		return builder
				.stream(TOPIC_DATA_AGG_MINUTE, Consumed.with(
						serdeRegistry.getSerde(SymbolTimestampKey.class), 
						serdeRegistry.getSerde(OHLCVData.class))
						.withOffsetResetPolicy(AutoOffsetReset.LATEST));
	}
	
	@Singleton @Named(TOPIC_DATA_WINDOWED_MINUTE)
	KStream<SymbolTimestampKey, OHLCVData> dataWinMinuteStream(final ConfiguredStreamBuilder builder)
	throws InterruptedException, ExecutionException
	{
		data15Sec.map((data15Key, data15Value) -> {
			return new KeyValue<>(data15Key.getSymbol(), data15Value);
		})
		.through(TOPIC_DATA_15SECS_GROUPED_SYMBOL, Produced.with(
				serdeRegistry.getSerde(String.class), 
				serdeRegistry.getSerde(OHLCVData.class)))
		.groupByKey()
		.windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1)))
		.<OHLCVData>aggregate(() -> null, (key, ohlcvData, aggregateOhlcvData ) -> {
			
			if(aggregateOhlcvData == null) {
				return ohlcvData;
			}
			return aggregateOhlcvData.aggregateWith(ohlcvData);
		}, Materialized
				.<String, OHLCVData, WindowStore<Bytes,byte[]>>as(STORE_SYMBOL_AGGREGATE)
				.withKeySerde(serdeRegistry.getSerde(String.class))
				.withValueSerde(serdeRegistry.getSerde(OHLCVData.class))
				.withCachingDisabled())
		.toStream()
		.map((windowedSymbol, aggregatedValue) -> {
			// TODO copy the object to manipulate the key
			aggregatedValue.getKey().setTimestamp(windowedSymbol.window().start());
			
			return new KeyValue<>(
					SymbolTimestampKey.builder()
					.symbol(windowedSymbol.key())
					.timestamp(windowedSymbol.window().start())
					.build(),
					aggregatedValue);
		})
		.to(TOPIC_DATA_WINDOWED_MINUTE, Produced.with(
				serdeRegistry.getSerde(SymbolTimestampKey.class), 
				serdeRegistry.getSerde(OHLCVData.class)));
		
		return builder
				.stream(TOPIC_DATA_WINDOWED_MINUTE, Consumed.with(
						serdeRegistry.getSerde(SymbolTimestampKey.class), 
						serdeRegistry.getSerde(OHLCVData.class))
						.withOffsetResetPolicy(AutoOffsetReset.LATEST));
	}
}
