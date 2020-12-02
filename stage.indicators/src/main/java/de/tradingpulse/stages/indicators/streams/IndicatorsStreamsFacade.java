package de.tradingpulse.stages.indicators.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;
import de.tradingpulse.stages.indicators.recordtypes.SSTOCRecord;
import lombok.Getter;

@Singleton
@Getter
public class IndicatorsStreamsFacade {

	// daily streams ----------------------------------------------------------

	@Inject @Named(EMAStreams.TOPIC_EMA_13_DAILY)
	private KStream<SymbolTimestampKey, DoubleRecord> ema13DailyStream;
	private final String ema13DailyStreamName = EMAStreams.TOPIC_EMA_13_DAILY;
	
	@Inject @Named(EMAStreams.TOPIC_EMA_26_DAILY)
	private KStream<SymbolTimestampKey, DoubleRecord> ema26DailyStream;
	private final String ema26DailyStreamName = EMAStreams.TOPIC_EMA_26_DAILY;
	
	@Inject @Named(MACDStreams.TOPIC_MACD_12_26_9_DAILY)
	private KStream<SymbolTimestampKey, MACDHistogramRecord> macd12269DailyStream;
	private final String macd12269DailyStreamName = MACDStreams.TOPIC_MACD_12_26_9_DAILY;

	@Inject @Named(SSTOCStreams.TOPIC_SSTOC_5_5_3_DAILY)
	private KStream<SymbolTimestampKey, SSTOCRecord> sstoc553DailyStream;
	private final String sstoc553DailyStreamName = SSTOCStreams.TOPIC_SSTOC_5_5_3_DAILY;
	
	// weekly incremental streams ---------------------------------------------

	@Inject @Named(EMAStreams.TOPIC_EMA_13_WEEKLY)
	private KStream<SymbolTimestampKey, DoubleRecord> ema13WeeklyStream;
	private final String ema13WeeklyStreamName = EMAStreams.TOPIC_EMA_13_WEEKLY;
	
	@Inject @Named(EMAStreams.TOPIC_EMA_26_WEEKLY)
	private KStream<SymbolTimestampKey, DoubleRecord> ema26WeeklyStream;
	private final String ema26WeeklyStreamName = EMAStreams.TOPIC_EMA_26_WEEKLY;
	
	@Inject @Named(MACDStreams.TOPIC_MACD_12_26_9_WEEKLY)
	private KStream<SymbolTimestampKey, MACDHistogramRecord> macd12269WeeklyStream;
	private final String macd12269WeeklyStreamName = MACDStreams.TOPIC_MACD_12_26_9_WEEKLY;

	@Inject @Named(SSTOCStreams.TOPIC_SSTOC_5_5_3_WEEKLY)
	private KStream<SymbolTimestampKey, SSTOCRecord> sstoc553WeeklyStream;
	private final String sstoc553WeeklyStreamName = SSTOCStreams.TOPIC_SSTOC_5_5_3_WEEKLY;
	
}
