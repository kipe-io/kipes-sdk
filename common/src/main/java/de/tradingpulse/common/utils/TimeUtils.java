package de.tradingpulse.common.utils;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TimeUtils {

	private TimeUtils() {}
	
	/**
	 * Returns the timestamp of the first second of the week the given timestamp is at.
	 */
	public static final Long getStartOfWeekTimestampUTC(Long timestampMillisUTC) {
		
		long epochSecond = timestampMillisUTC / 1000;
		return LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC)
				.atOffset(ZoneOffset.UTC)
				.toZonedDateTime()
				.with(DayOfWeek.MONDAY)
				.withHour(0)
				.withMinute(0)
				.withSecond(0)
				.withNano(0)
				.toEpochSecond() * 1000;
	}

	/**
	 * Returns the timestamp of the fist second of the minute the given timestamp is at. 
	 */
	public static final Long getStartOfMinuteTimestampUTC(Long timestampMillisUTC) {
		
		long epochSecond = timestampMillisUTC / 1000;
		return LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC)
				.atOffset(ZoneOffset.UTC)
				.toZonedDateTime()
				.withSecond(0)
				.withNano(0)
				.toEpochSecond() * 1000;
	}
}
