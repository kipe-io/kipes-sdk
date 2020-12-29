package de.tradingpulse.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class MathUtilsTest {

	// ------------------------------------------------------------------------
	// test round
	// ------------------------------------------------------------------------

	@ParameterizedTest
	@MethodSource("createRoundTests")
	void test_round(double value, int digits, double result) {
		assertEquals(result, MathUtils.round(value, digits));
	}

	static Stream<Arguments> createRoundTests() {
		return Stream.of(
				arguments(	0.123456789,	0,	0.0),
				arguments(	0.123456789,	1,	0.1),
				arguments(	0.123456789,	2,	0.12),
				arguments(	0.123456789,	3,	0.123),
				arguments(	0.123456789,	4,	0.1235),
				arguments( -0.123456789,	4, -0.1235),
				arguments( -0.123456789,	3, -0.123),
				arguments( -0.123456789,	2, -0.12),
				arguments( -0.123456789,	1, -0.1),
				arguments( -0.123456789,	0,  0.0)
			);
	}
}
