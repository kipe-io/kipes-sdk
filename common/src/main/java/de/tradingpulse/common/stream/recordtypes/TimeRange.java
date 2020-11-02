package de.tradingpulse.common.stream.recordtypes;

import java.util.function.UnaryOperator;

import de.tradingpulse.common.utils.TimeUtils;

public enum TimeRange {

	MILLISECOND((timestampMillisUTC) -> timestampMillisUTC),
	MINUTE(TimeUtils::getStartOfMinuteTimestampUTC),
	DAY(TimeUtils::getStartOfDayTimestampUTC),
	WEEK(TimeUtils::getStartOfWeekTimestampUTC);
	
	private final UnaryOperator<Long> converterFunc;
	
	private TimeRange(UnaryOperator<Long> converterFunc) {
		this.converterFunc = converterFunc;
	}
	
	public long getStartOfTimeRangeFor(long timestampMillisUTC) {
		return this.converterFunc.apply(timestampMillisUTC);
	}
}
