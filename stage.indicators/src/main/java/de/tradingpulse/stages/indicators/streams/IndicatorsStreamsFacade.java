package de.tradingpulse.stages.indicators.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.data.DoubleData;
import de.tradingpulse.common.stream.data.MACDHistogramData;
import de.tradingpulse.common.stream.data.SymbolTimestampKey;
import lombok.Getter;

@Singleton
@Getter
public class IndicatorsStreamsFacade {

	// daily streams ----------------------------------------------------------

	@Inject @Named(EMAStreams.TOPIC_EMA_13_DAILY)
	private KStream<SymbolTimestampKey, DoubleData> ema13DailyStream;
	private final String ema13DailyStreamName = EMAStreams.TOPIC_EMA_13_DAILY;
	
	@Inject @Named(EMAStreams.TOPIC_EMA_26_DAILY)
	private KStream<SymbolTimestampKey, DoubleData> ema26DailyStream;
	private final String ema26DailyStreamName = EMAStreams.TOPIC_EMA_26_DAILY;
	
	@Inject @Named(MACDStreams.TOPIC_MACD_12_26_9_DAILY)
	private KStream<SymbolTimestampKey, MACDHistogramData> macd12269DailyStream;
	private final String macd12269DailyStreamName = MACDStreams.TOPIC_MACD_12_26_9_DAILY;

	// weekly incremental streams ---------------------------------------------

	@Inject @Named(EMAStreams.TOPIC_EMA_13_WEEKLY_INCREMENTAL)
	private KStream<SymbolTimestampKey, DoubleData> ema13WeeklyIncrementalStream;
	private final String ema13WeeklyIncrementalStreamName = EMAStreams.TOPIC_EMA_13_WEEKLY_INCREMENTAL;
	
	@Inject @Named(EMAStreams.TOPIC_EMA_26_WEEKLY_INCREMENTAL)
	private KStream<SymbolTimestampKey, DoubleData> ema26WeeklyIncrementalStream;
	private final String ema26WeeklyIncrementalStreamName = EMAStreams.TOPIC_EMA_26_WEEKLY_INCREMENTAL;
	
	@Inject @Named(MACDStreams.TOPIC_MACD_12_26_9_WEEKLY_INCREMENTAL)
	private KStream<SymbolTimestampKey, MACDHistogramData> macd12269WeeklyIncrementalStream;
	private final String macd12269WeeklyIncrementalStreamName = MACDStreams.TOPIC_MACD_12_26_9_WEEKLY_INCREMENTAL;
	
}
