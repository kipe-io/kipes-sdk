/*
 * Kipes SDK for Kafka - The High-Level Event Processing SDK.
 * Copyright © 2023 kipe.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.kipe.common.utils;

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
