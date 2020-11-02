package de.tradingpulse.stage.systems.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import lombok.Getter;

@Singleton
@Getter
public final class SystemsStreamsFacade {

	@Inject @Named(ImpulseStreams.TOPIC_IMPULSE_DAILY)
    private KStream<SymbolTimestampKey, ImpulseRecord> impulseDailyStream;
	private final String impulseDailyStreamName = ImpulseStreams.TOPIC_IMPULSE_DAILY;
	
	@Inject @Named(ImpulseStreams.TOPIC_IMPULSE_WEEKLY)
    private KStream<SymbolTimestampKey, ImpulseRecord> impulseWeeklyStream;
	private final String impulseWeeklyStreamName = ImpulseStreams.TOPIC_IMPULSE_WEEKLY;

}
