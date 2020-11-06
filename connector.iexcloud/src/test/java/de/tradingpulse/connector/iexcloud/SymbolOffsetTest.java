package de.tradingpulse.connector.iexcloud;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

class SymbolOffsetTest {

	// ------------------------------------------------------------------------
	// test_compare
	// ------------------------------------------------------------------------
	
	@Test
	void test_compare__null_offsets_return_symbol_compare() {
		SymbolOffset s1 = new SymbolOffset("s1", null);
		SymbolOffset s2 = new SymbolOffset("s2", null);
		
		assertEquals(s1.symbol.compareTo(s2.symbol), s1.compareTo(s2));
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
		
		assertEquals(s1.symbol.compareTo(s2.symbol), s1.compareTo(s2));
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
