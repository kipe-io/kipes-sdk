package de.tradingpulse.common.utils;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class TimeUtils {

	/** yyyy-MM-dd */
	public static final DateTimeFormatter FORMATTER_YYYY_MM_DD = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
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
	 * Returns the timestamp of the first second of the day the given timestamp is at. 
	 */
	public static final Long getStartOfDayTimestampUTC(Long timestampMillisUTC) {
		
		long epochSecond = timestampMillisUTC / 1000;
		return LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC)
				.atOffset(ZoneOffset.UTC)
				.toZonedDateTime()
				.withHour(0)
				.withMinute(0)
				.withSecond(0)
				.withNano(0)
				.toEpochSecond() * 1000;
	}
	
	/**
	 * Returns the timestamp of the first second of the minute the given timestamp is at. 
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
	
	public static final long getTimestampDaysBeforeNow(int daysBefore) {
		return LocalDate.now()
				.minus(daysBefore, ChronoUnit.DAYS)
				.atStartOfDay(ZoneId.of("UTC"))
				.toEpochSecond() * 1000;
	}
}
