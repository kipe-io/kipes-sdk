package de.tradingpulse.common.stream.aggregates;

import static de.tradingpulse.common.stream.data.TradingDirection.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.tradingpulse.common.stream.data.ImpulseData;
import de.tradingpulse.common.stream.data.SymbolTimestampKey;
import de.tradingpulse.common.stream.data.TradingDirection;
import de.tradingpulse.common.stream.data.TradingScreenData;

class TradingScreenAggregateTest {

	@Test
	void test_aggregate__returns_null() {
		assertNull(new TradingScreenAggregate().aggregate(null, null));
		assertNull(new TradingScreenAggregate().aggregate(new ImpulseData(), null));
		assertNull(new TradingScreenAggregate().aggregate(null, new ImpulseData()));
	}

	@Test
	void test_aggregate__calculates_correctly_long() {
		assertEquals(LONG, new TradingScreenAggregate().aggregate(impulse(LONG), impulse(LONG)).getDirection());
	}

	@Test
	void test_aggregate__calculates_correctly_short() {
		assertEquals(SHORT, new TradingScreenAggregate().aggregate(impulse(SHORT), impulse(SHORT)).getDirection());
	}
	
	@Test
	void test_aggregate__calculates_correctly_neutral() {
		assertEquals(NEUTRAL, new TradingScreenAggregate().aggregate(impulse(NEUTRAL), impulse(NEUTRAL)).getDirection());
		assertEquals(NEUTRAL, new TradingScreenAggregate().aggregate(impulse(NEUTRAL), impulse(LONG)).getDirection());
		assertEquals(NEUTRAL, new TradingScreenAggregate().aggregate(impulse(NEUTRAL), impulse(SHORT)).getDirection());
		assertEquals(NEUTRAL, new TradingScreenAggregate().aggregate(impulse(LONG), impulse(NEUTRAL)).getDirection());
		assertEquals(NEUTRAL, new TradingScreenAggregate().aggregate(impulse(SHORT), impulse(NEUTRAL)).getDirection());
		assertEquals(NEUTRAL, new TradingScreenAggregate().aggregate(impulse(SHORT), impulse(LONG)).getDirection());
		assertEquals(NEUTRAL, new TradingScreenAggregate().aggregate(impulse(LONG), impulse(SHORT)).getDirection());
	}
	
	@Test
	void test_aggregate__forwards_daily_key() {
		SymbolTimestampKey key = new SymbolTimestampKey("symbol", 1234);
		ImpulseData daily = impulse(NEUTRAL);
		daily.setKey(key);
		
		assertEquals(key, new TradingScreenAggregate().aggregate(impulse(LONG), daily).getKey());
	}
	
	@Test
	void test_aggregate__returns_null_lastDirection() {
		assertNull(new TradingScreenAggregate().aggregate(impulse(NEUTRAL), impulse(NEUTRAL)).getLastDirection());
	}
	
	@Test
	void test_aggregate__calculates_correctly_lastDirection_also_with_serde()
	throws JsonProcessingException
	{
		
		SymbolTimestampKey key = new SymbolTimestampKey("symbol", 0);
		TradingDirection lastDirection = NEUTRAL;
		
		TradingScreenData tradingScreen = TradingScreenData.builder()
				.key(key)
				.lastDirection(SHORT)
				.direction(lastDirection)
				.build();
		
		TradingScreenAggregate a = new TradingScreenAggregate(tradingScreen);
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(a);
		a = mapper.readValue(json, TradingScreenAggregate.class);
		
		assertEquals(lastDirection, a.aggregate(impulse(LONG), impulse(LONG)).getLastDirection());
		
	}
	
	private ImpulseData impulse(TradingDirection direction) {
		return new ImpulseData(null, direction, null);
	}
}
