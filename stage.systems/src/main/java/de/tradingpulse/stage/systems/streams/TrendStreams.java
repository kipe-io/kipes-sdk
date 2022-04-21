package de.tradingpulse.stage.systems.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.SystemsStageConstants;
import de.tradingpulse.streams.kafka.factories.AbstractStreamFactory;
import de.tradingpulse.streams.recordtypes.GenericRecord;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;

@Factory
class TrendStreams extends AbstractStreamFactory {

	static final String TOPIC_TREND_EMA_DAILY = SystemsStageConstants.STAGE_NAME + "-" + "trend_ema_daily";
	static final String TOPIC_TREND_EMA_WEEKLY = SystemsStageConstants.STAGE_NAME + "-" + "trend_ema_weekly";

	static final String TOPIC_TREND_MACD_DAILY = SystemsStageConstants.STAGE_NAME + "-" + "trend_macd_daily";
	static final String TOPIC_TREND_MACD_WEEKLY = SystemsStageConstants.STAGE_NAME + "-" + "trend_macd_weekly";

	static final String TOPIC_TREND_SSTOC_DAILY = SystemsStageConstants.STAGE_NAME + "-" + "trend_sstoc_daily";
	static final String TOPIC_TREND_SSTOC_WEEKLY = SystemsStageConstants.STAGE_NAME + "-" + "trend_sstoc_weekly";

	static final String TOPIC_TRENDS_DAILY = SystemsStageConstants.STAGE_NAME + "-" + "trends_daily";
	static final String TOPIC_TRENDS_WEEKLY = SystemsStageConstants.STAGE_NAME + "-" + "trends_weekly";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_TREND_EMA_DAILY,
				TOPIC_TREND_EMA_WEEKLY,
				TOPIC_TREND_MACD_DAILY,
				TOPIC_TREND_MACD_WEEKLY,
				TOPIC_TREND_SSTOC_DAILY,
				TOPIC_TREND_SSTOC_WEEKLY,
				TOPIC_TRENDS_DAILY,
				TOPIC_TRENDS_WEEKLY
		};
	}
	
	@Singleton
	@Named(TOPIC_TREND_EMA_DAILY)
	KStream<SymbolTimestampKey, GenericRecord> trendEMADailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_TREND_EMA_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(GenericRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_TREND_EMA_WEEKLY)
	KStream<SymbolTimestampKey, GenericRecord> trendEMAWeeklyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_TREND_EMA_WEEKLY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(GenericRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_TREND_MACD_DAILY)
	KStream<SymbolTimestampKey, GenericRecord> trendMACDDailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_TREND_MACD_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(GenericRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_TREND_MACD_WEEKLY)
	KStream<SymbolTimestampKey, GenericRecord> trendMACDWeeklyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_TREND_MACD_WEEKLY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(GenericRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_TREND_SSTOC_DAILY)
	KStream<SymbolTimestampKey, GenericRecord> trendSSTOCDailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_TREND_SSTOC_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(GenericRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_TREND_SSTOC_WEEKLY)
	KStream<SymbolTimestampKey, GenericRecord> trendSSTOCWeeklyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_TREND_SSTOC_WEEKLY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(GenericRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_TRENDS_DAILY)
	KStream<SymbolTimestampKey, GenericRecord> trendsDailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_TRENDS_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(GenericRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
	@Singleton
	@Named(TOPIC_TRENDS_WEEKLY)
	KStream<SymbolTimestampKey, GenericRecord> trendsWeeklyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_TRENDS_WEEKLY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(GenericRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
	}
	
}
