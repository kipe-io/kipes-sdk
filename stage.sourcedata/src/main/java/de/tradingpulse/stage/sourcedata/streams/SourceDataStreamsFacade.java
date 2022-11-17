package de.tradingpulse.stage.sourcedata.streams;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRawRecord;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.Getter;

@Singleton
@Getter
public class SourceDataStreamsFacade {

	@Inject @Named(OHLCVDailyRawStream.TOPIC_OHLCV_DAILY_RAW)
    private KStream<String, OHLCVRawRecord> ohlcvDailyRawStream;
	private final String ohlcvDailyRawStreamName = OHLCVDailyRawStream.TOPIC_OHLCV_DAILY_RAW;
	
	@Inject @Named(OHLCVStreams.TOPIC_OHLCV_DAILY)
    private KStream<SymbolTimestampKey, OHLCVRecord> ohlcvDailyStream;
	public static final String TOPIC_OHLCV_DAILY = OHLCVStreams.TOPIC_OHLCV_DAILY;

	@Inject @Named(OHLCVStreams.TOPIC_OHLCV_WEEKLY)
    private KStream<SymbolTimestampKey, OHLCVRecord> ohlcvWeeklyStream;
	private final String ohlcvWeeklyStreamName = OHLCVStreams.TOPIC_OHLCV_WEEKLY;

}
