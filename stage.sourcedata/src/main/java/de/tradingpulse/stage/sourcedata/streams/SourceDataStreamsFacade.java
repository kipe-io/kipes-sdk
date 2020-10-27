package de.tradingpulse.stage.sourcedata.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.data.OHLCVData;
import de.tradingpulse.common.stream.data.OHLCVDataRaw;
import de.tradingpulse.common.stream.data.SymbolTimestampKey;
import lombok.Getter;

@Singleton
@Getter
public final class SourceDataStreamsFacade {

	@Inject @Named(OHLCVDailyRawStream.TOPIC_OHLCV_DAILY_RAW)
    private KStream<String, OHLCVDataRaw> ohlcvDailyRawStream;
	private final String ohlcvDailyRawStreamName = OHLCVDailyRawStream.TOPIC_OHLCV_DAILY_RAW;
	
	@Inject @Named(OHLCVDailyStream.TOPIC_OHLCV_DAILY)
    private KStream<SymbolTimestampKey, OHLCVData> ohlcvDailyStream;
	private final String ohlcvDailyStreamName = OHLCVDailyStream.TOPIC_OHLCV_DAILY;

	@Inject @Named(OHLCVWeeklyIncrementalStream.TOPIC_OHLCV_WEEKLY_INCREMENTAL)
    private KStream<SymbolTimestampKey, OHLCVData> ohlcvWeeklyIncrementalStream;
	private final String ohlcvWeeklyIncrementalStreamName = OHLCVWeeklyIncrementalStream.TOPIC_OHLCV_WEEKLY_INCREMENTAL;

}
