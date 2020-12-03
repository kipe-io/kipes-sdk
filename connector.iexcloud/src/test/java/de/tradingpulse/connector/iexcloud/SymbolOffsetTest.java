package de.tradingpulse.connector.iexcloud;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

class SymbolOffsetTest {

	// ------------------------------------------------------------------------
	// test_isLastFetchedDateBefore
	// ------------------------------------------------------------------------
	
	@Test
	void test_isLastFetchedDateBefore__null_lastFetchedDate_is_before() {
		SymbolOffset s = new SymbolOffset("s", null);
		
		assertTrue(s.isLastFetchedDateBefore(null)); // yea, even not before null ;)
		assertTrue(s.isLastFetchedDateBefore(LocalDate.now())); // but for the curious
	}
	
	@Test
	void test_isLastFetchedDateBefore__earlier_lastFetchedDate_is_before() {
		LocalDate now = LocalDate.now();
		SymbolOffset s = new SymbolOffset("s", now.minusDays(1));
		
		assertTrue(s.isLastFetchedDateBefore(now)); 
	}
	
	@Test
	void test_isLastFetchedDateBefore__same_lastFetchedDate_is_not_before() {
		LocalDate now = LocalDate.now();
		SymbolOffset s = new SymbolOffset("s", now);
		
		assertFalse(s.isLastFetchedDateBefore(now)); 
	}
	
	@Test
	void test_isLastFetchedDateBefore__later_lastFetchedDate_is_not_before() {
		LocalDate now = LocalDate.now();
		SymbolOffset s = new SymbolOffset("s", now.plusDays(1));
		
		assertFalse(s.isLastFetchedDateBefore(now)); 
	}

	// ------------------------------------------------------------------------
	// test_isLastFetchAttemptBefore
	// ------------------------------------------------------------------------
	
	@Test
	void test_isLastFetchAttemptBefore__null_lastFetchedDate_is_before() {
		SymbolOffset s = new SymbolOffset("s", null);
		s.setLastFetchAttemptDateTime(null);
		
		assertTrue(s.isLastFetchAttemptBefore(null)); // yea, even not before null ;)
		assertTrue(s.isLastFetchAttemptBefore(LocalDateTime.now())); // but for the curious
	}
	
	@Test
	void test_isLastFetchAttemptBefore__earlier_lastFetchedDate_is_before() {
		LocalDateTime now = LocalDateTime.now();
		SymbolOffset s = new SymbolOffset("s", null);
		s.setLastFetchAttemptDateTime(now.minusNanos(1));
		
		assertTrue(s.isLastFetchAttemptBefore(now)); 
	}
	
	@Test
	void test_isLastFetchAttemptBefore__same_lastFetchedDate_is_not_before() {
		LocalDateTime now = LocalDateTime.now();
		SymbolOffset s = new SymbolOffset("s", null);
		s.setLastFetchAttemptDateTime(now);
		
		assertFalse(s.isLastFetchAttemptBefore(now)); 
	}
	
	@Test
	void test_isLastFetchAttemptBefore__later_lastFetchedDate_is_not_before() {
		LocalDateTime now = LocalDateTime.now();
		SymbolOffset s = new SymbolOffset("s", null);
		s.setLastFetchAttemptDateTime(now.plusNanos(1));
		
		assertFalse(s.isLastFetchAttemptBefore(now)); 
	}
	
	// ------------------------------------------------------------------------
	// test_compare
	// ------------------------------------------------------------------------
	
	@Test
	void test_compare__null_offsets_return_symbol_compare() {
		SymbolOffset s1 = new SymbolOffset("s1", null);
		SymbolOffset s2 = new SymbolOffset("s2", null);
		
		assertEquals(s1.getSymbol().compareTo(s2.getSymbol()), s1.compareTo(s2));
	}
	
	@Test
	void test_compare__this_null_offset_return_1() {
		SymbolOffset s1 = new SymbolOffset("s1", null);
		SymbolOffset s2 = new SymbolOffset("s2", LocalDate.now());
		
		assertEquals( 1, s1.compareTo(s2));
	}
	
	@Test
	void test_compare__other_null_offset_return_minus_1() {
		SymbolOffset s1 = new SymbolOffset("s1", LocalDate.now());
		SymbolOffset s2 = new SymbolOffset("s2", null);
		
		assertEquals(-1, s1.compareTo(s2));
	}
	
	@Test
	void test_compare__same_offset_return_symbol_compare() {
		LocalDate now = LocalDate.now();
		SymbolOffset s1 = new SymbolOffset("s1", now);
		SymbolOffset s2 = new SymbolOffset("s2", now);
		
		assertEquals(s1.getSymbol().compareTo(s2.getSymbol()), s1.compareTo(s2));
	}
	
	@Test
	void test_compare__other_offset_returns_reflexive() {
		LocalDate now = LocalDate.now();
		LocalDate later = now.plusDays(1);
		
		SymbolOffset s1 = new SymbolOffset("s1", now);
		SymbolOffset s2 = new SymbolOffset("s2", later);
		
		assertEquals( 1, s1.compareTo(s2));
		assertEquals(-1, s2.compareTo(s1));
	}

}
