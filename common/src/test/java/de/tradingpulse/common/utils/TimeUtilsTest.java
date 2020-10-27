package de.tradingpulse.common.utils;

import static org.junit.jupiter.api.Assertions.*;

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
	void test_getStartOfMinuteTimestampUTC() {
		// Tuesday, 2020-09-01 09:34:31 UTC
		long timestampMillisUTC = 1598952871000L;
		
		// Monday, 2020-09-01 09:34:00 UTC
		long startOfWeek = 1598952840000L;
		
		assertEquals(startOfWeek, TimeUtils.getStartOfMinuteTimestampUTC(timestampMillisUTC));
	}

}
