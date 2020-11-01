package de.tradingpulse.stage.sourcedata.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRawRecord;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import lombok.Getter;

@Singleton
@Getter
public final class SourceDataStreamsFacade {

	@Inject @Named(OHLCVDailyRawStream.TOPIC_OHLCV_DAILY_RAW)
    private KStream<String, OHLCVRawRecord> ohlcvDailyRawStream;
	private final String ohlcvDailyRawStreamName = OHLCVDailyRawStream.TOPIC_OHLCV_DAILY_RAW;
	
	@Inject @Named(OHLCVStreams.TOPIC_OHLCV_DAILY)
    private KStream<SymbolTimestampKey, OHLCVRecord> ohlcvDailyStream;
	private final String ohlcvDailyStreamName = OHLCVStreams.TOPIC_OHLCV_DAILY;

	@Inject @Named(OHLCVStreams.TOPIC_OHLCV_WEEKLY)
    private KStream<SymbolTimestampKey, OHLCVRecord> ohlcvWeeklyStream;
	private final String ohlcvWeeklyStreamName = OHLCVStreams.TOPIC_OHLCV_WEEKLY;

}
