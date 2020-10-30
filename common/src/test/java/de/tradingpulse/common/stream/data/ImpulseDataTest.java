package de.tradingpulse.common.stream.data;

import static de.tradingpulse.common.stream.data.TradingDirection.LONG;
import static de.tradingpulse.common.stream.data.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.data.TradingDirection.SHORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ImpulseDataTest {

	@ParameterizedTest
	@MethodSource("createTradingDirections")
	void test_getChangeTradingDirection(TradingDirection last, TradingDirection current, TradingDirection change) {
		ImpulseData data = ImpulseData.builder()
				.lastTradingDirection(last)
				.tradingDirection(current)
				.build();
		
		assertEquals(change, data.getChangeTradingDirection());
	}

	static Stream<Arguments> createTradingDirections() {
		return Stream.of(
				arguments(	null,	null, 	NEUTRAL),
				arguments(	null,	SHORT,	NEUTRAL),
				arguments(	null,	NEUTRAL,NEUTRAL),
				arguments(	null,	LONG,	NEUTRAL),
				
				arguments(	SHORT,	null,	NEUTRAL),
				arguments(	SHORT,	SHORT,	NEUTRAL),
				arguments(	SHORT,	NEUTRAL,LONG),
				arguments(	SHORT,	LONG,	LONG),
				
				arguments(	NEUTRAL,null,	NEUTRAL),
				arguments(	NEUTRAL,SHORT,	SHORT),
				arguments(	NEUTRAL,NEUTRAL,NEUTRAL),
				arguments(	NEUTRAL,LONG,	LONG),
				
				arguments(	LONG,	null,	NEUTRAL),
				arguments(	LONG,	SHORT,	SHORT),
				arguments(	LONG,	NEUTRAL,SHORT),
				arguments(	LONG,	LONG,	NEUTRAL)
			);
	}
}
