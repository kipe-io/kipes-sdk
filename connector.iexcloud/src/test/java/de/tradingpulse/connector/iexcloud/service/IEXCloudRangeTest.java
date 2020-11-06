package de.tradingpulse.connector.iexcloud.service;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

class IEXCloudRangeTest {

	@Test
	void test_isCovered() {
		// TODO test whether we are only a few days into the next day and wait jic
		
		LocalDate now = LocalDate.now();
		
		assertTrue(IEXCloudRange.M6.isCovered(now.minusMonths(6)));
		assertFalse(IEXCloudRange.M6.isCovered(now.minusMonths(6).minusDays(1)));
	}

	@Test
	void test_getExcessDays__exception_when_not_covered() {
		// TODO test whether we are only a few days into the next day and wait jic
		
		LocalDate dateInPast = LocalDate.now().minusMonths(6).minusDays(1);
		
		assertThrows(IllegalArgumentException.class, () -> {
			IEXCloudRange.M6.getExcessDays(dateInPast);
		});		
	}

	@Test
	void test_getExcessDays() {
		// TODO test whether we are only a few days into the next day and wait jic
		
		LocalDate dateInPast = LocalDate.now().minusMonths(6).plusDays(1);

		assertEquals(1, IEXCloudRange.M6.getExcessDays(dateInPast));
	}
	
	@Test
	void test_findLeastExcessDaysRange__null_when_not_covered() {
		// TODO test whether we are only a few days into the next day and wait jic
		
		assertFalse(IEXCloudRange.findLeastExcessDaysRange(LocalDate.now().minusYears(15).minusDays(1)).isPresent());
	}
	
	@Test
	void test_findLeastExcessDaysRange() {
		// TODO test whether we are only a few days into the next day and wait jic
		
		assertEquals(IEXCloudRange.M6, IEXCloudRange.findLeastExcessDaysRange(LocalDate.now().minusMonths(6).plusDays(1)).get());
	}
}
