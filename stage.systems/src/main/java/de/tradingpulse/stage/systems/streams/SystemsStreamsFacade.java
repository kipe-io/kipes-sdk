package de.tradingpulse.stage.systems.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.streams.recordtypes.GenericRecord;
import lombok.Getter;

@Singleton
@Getter
public final class SystemsStreamsFacade {

	@Inject @Named(ImpulseStreams.TOPIC_IMPULSE_DAILY)
    private KStream<SymbolTimestampKey, ImpulseRecord> impulseDailyStream;
	public static final String TOPIC_IMPULSE_DAILY = ImpulseStreams.TOPIC_IMPULSE_DAILY;
	
	@Inject @Named(ImpulseStreams.TOPIC_IMPULSE_WEEKLY)
    private KStream<SymbolTimestampKey, ImpulseRecord> impulseWeeklyStream;
	public static final String TOPIC_IMPULSE_WEEKLY = ImpulseStreams.TOPIC_IMPULSE_WEEKLY;

	@Inject @Named(TrendStreams.TOPIC_TREND_EMA_DAILY)
    private KStream<SymbolTimestampKey, GenericRecord> trendEMADailyStream;
	public static final String TOPIC_TREND_EMA_DAILY = TrendStreams.TOPIC_TREND_EMA_DAILY;

	@Inject @Named(TrendStreams.TOPIC_TREND_EMA_WEEKLY)
    private KStream<SymbolTimestampKey, GenericRecord> trendEMAWeeklyStream;
	public static final String TOPIC_TREND_EMA_WEEKLY = TrendStreams.TOPIC_TREND_EMA_WEEKLY;

	@Inject @Named(TrendStreams.TOPIC_TREND_MACD_DAILY)
    private KStream<SymbolTimestampKey, GenericRecord> trendMACDDailyStream;
	public static final String TOPIC_TREND_MACD_DAILY = TrendStreams.TOPIC_TREND_MACD_DAILY;

	@Inject @Named(TrendStreams.TOPIC_TREND_MACD_WEEKLY)
    private KStream<SymbolTimestampKey, GenericRecord> trendMACDWeeklyStream;
	public static final String TOPIC_TREND_MACD_WEEKLY = TrendStreams.TOPIC_TREND_MACD_WEEKLY;

	@Inject @Named(TrendStreams.TOPIC_TREND_SSTOC_DAILY)
    private KStream<SymbolTimestampKey, GenericRecord> trendSSTOCDailyStream;
	public static final String TOPIC_TREND_SSTOC_DAILY = TrendStreams.TOPIC_TREND_SSTOC_DAILY;

	@Inject @Named(TrendStreams.TOPIC_TREND_SSTOC_WEEKLY)
    private KStream<SymbolTimestampKey, GenericRecord> trendSSTOCWeeklyStream;
	public static final String TOPIC_TREND_SSTOC_WEEKLY = TrendStreams.TOPIC_TREND_SSTOC_WEEKLY;

	@Inject @Named(TrendStreams.TOPIC_TRENDS_DAILY)
    private KStream<SymbolTimestampKey, GenericRecord> trendsDailyStream;
	public static final String TOPIC_TRENDS_DAILY = TrendStreams.TOPIC_TRENDS_DAILY;

	@Inject @Named(TrendStreams.TOPIC_TRENDS_WEEKLY)
    private KStream<SymbolTimestampKey, GenericRecord> trendsWeeklyStream;
	public static final String TOPIC_TRENDS_WEEKLY = TrendStreams.TOPIC_TRENDS_WEEKLY;
	
}
