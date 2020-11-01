package de.tradingpulse.common.stream.aggregates;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.tradingpulse.common.stream.recordtypes.DoubleData;
import de.tradingpulse.common.stream.recordtypes.ImpulseData;
import de.tradingpulse.common.stream.recordtypes.MACDHistogramData;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;

class ImpulseAggregateTest {

	@Test
	void test_aggregate__returns_null() {
		assertNull(new ImpulseAggregate().aggregate(null, null));
		assertNull(new ImpulseAggregate().aggregate(new DoubleData(), null));
		assertNull(new ImpulseAggregate().aggregate(null, new MACDHistogramData()));
		assertNull(new ImpulseAggregate().aggregate(new DoubleData(), new MACDHistogramData()));
	}	
	
	@Test
	void test_aggregate__calculates_correctly_long() {
		assertEquals(LONG, new ImpulseAggregate().aggregate(emaData(1.0), macdData(1.0)).getTradingDirection());
	}

	@Test
	void test_aggregate__calculates_correctly_short() {
		assertEquals(SHORT, new ImpulseAggregate().aggregate(emaData(-1.0), macdData(-1.0)).getTradingDirection());
	}

	@Test
	void test_aggregate__calculates_correctly_neutral() {
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaData( 0.0), macdData( 0.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaData( 0.0), macdData(-1.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaData( 0.0), macdData( 1.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaData(-1.0), macdData( 0.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaData( 1.0), macdData( 0.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaData(-1.0), macdData( 1.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaData( 1.0), macdData(-1.0)).getTradingDirection());
	}

	@Test
	void test_aggregate__calculates_correctly_lastDirection_also_with_serde()
	throws JsonProcessingException
	{
		TradingDirection lastDirection = NEUTRAL;
		
		ImpulseData impulseData = ImpulseData.builder()
				.lastTradingDirection(SHORT)
				.tradingDirection(lastDirection)
				.build();
		
		ImpulseAggregate a = new ImpulseAggregate(impulseData);
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(a);
		a = mapper.readValue(json, ImpulseAggregate.class);
		
		assertEquals(lastDirection, a.aggregate(emaData(1.0), macdData(1.0)).getLastTradingDirection());
	}
	
	private DoubleData emaData(double vChange) {
		return DoubleData.builder()
				.vChange(vChange)
				.build();
	}
	
	private MACDHistogramData macdData(double hChange) {
		return MACDHistogramData.builder()
				.hChange(hChange)
				.build();
	}
}
