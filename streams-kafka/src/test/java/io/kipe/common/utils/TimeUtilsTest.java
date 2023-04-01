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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TimeUtilsTest {

	@Test
	void test_getStartOfWeekTimestampUTC() {
		// Tuesday, 2020-09-01 09:34:31 UTC
		long timestampMillisUTC = 1598952871000L;
		
		// Monday, 2020-08-31 00:00:00 UTC
		long startOfWeek = 1598832000000L;
		
		assertEquals(startOfWeek, TimeUtils.getStartOfWeekTimestampUTC(timestampMillisUTC));
	}

	@Test
	void test_getStartOfDayTimestampUTC() {
		// Tuesday, 2020-09-01 09:34:31 UTC
		long timestampMillisUTC = 1598952871000L;
		
		// Tuesday, 2020-09-01 00:00:00 UTC
		long startOfDay = 1598918400000L;
		
		assertEquals(startOfDay, TimeUtils.getStartOfDayTimestampUTC(timestampMillisUTC));
	}

	@Test
	void test_getStartOfMinuteTimestampUTC() {
		// Tuesday, 2020-09-01 09:34:31 UTC
		long timestampMillisUTC = 1598952871000L;
		
		// Monday, 2020-09-01 09:34:00 UTC
		long startOfWeek = 1598952840000L;
		
		assertEquals(startOfWeek, TimeUtils.getStartOfMinuteTimestampUTC(timestampMillisUTC));
	}

}
