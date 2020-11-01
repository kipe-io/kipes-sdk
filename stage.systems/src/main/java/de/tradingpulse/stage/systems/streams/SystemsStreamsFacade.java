package de.tradingpulse.stage.systems.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.ImpulseData;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import lombok.Getter;

@Singleton
@Getter
public final class SystemsStreamsFacade {

	@Inject @Named(ImpulseStreams.TOPIC_IMPULSE_DAILY)
    private KStream<SymbolTimestampKey, ImpulseData> impulseDailyStream;
	private final String impulseDailyStreamName = ImpulseStreams.TOPIC_IMPULSE_DAILY;
	
	@Inject @Named(ImpulseStreams.TOPIC_IMPULSE_WEEKLY_INCREMENTAL)
    private KStream<SymbolTimestampKey, ImpulseData> impulseWeeklyIncrementalStream;
	private final String impulseWeeklyIncrementalStreamName = ImpulseStreams.TOPIC_IMPULSE_WEEKLY_INCREMENTAL;

}
