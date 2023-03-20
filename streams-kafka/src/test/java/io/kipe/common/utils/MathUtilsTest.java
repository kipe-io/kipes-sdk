package io.kipe.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kipe.common.utils.MathUtils;

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
				arguments(	1.123456789,	0,	1.0),
				arguments(	1.123456789,	1,	1.1),
				arguments(	1.123456789,	2,	1.12),
				arguments(	1.123456789,	3,	1.123),
				arguments(	1.123456789,	4,	1.1235),
				arguments( -1.123456789,	4, -1.1235),
				arguments( -1.123456789,	3, -1.123),
				arguments( -1.123456789,	2, -1.12),
				arguments( -1.123456789,	1, -1.1),
				arguments( -1.123456789,	0, -1.0)
			);
	}

	// ------------------------------------------------------------------------
	// test precision
	// ------------------------------------------------------------------------

	@ParameterizedTest
	@MethodSource("createPrecisisonTests")
	void test_precision(double value, int precision) {
		assertEquals(precision, MathUtils.getPrecision(value));
	}

	static Stream<Arguments> createPrecisisonTests() {
		return Stream.of(
				arguments( -5.0000000001,	10),
				arguments( -0.1234567891,	10),
				arguments( -5.000000001,	 9),
				arguments( -0.123456789,	 9),
				arguments( -5.1,			 1),
				arguments( -0.1,			 1),
				arguments(  0.1,			 1),
				arguments(  5.1,			 1),
				arguments(  0.123456789,	 9),
				arguments(  5.000000001,	 9),
				arguments(  0.1234567891,	10),
				arguments(  5.0000000001,	10),

				arguments(  0.0,			 0)
			);
	}
	
	
}
