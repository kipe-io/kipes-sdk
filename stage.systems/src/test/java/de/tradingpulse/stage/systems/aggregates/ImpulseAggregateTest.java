package de.tradingpulse.stage.systems.aggregates;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.recordtypes.ImpulseData;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;

class ImpulseAggregateTest {

	@Test
	void test_aggregate__returns_null() {
		assertNull(new ImpulseAggregate().aggregate(null, null));
		assertNull(new ImpulseAggregate().aggregate(new DoubleRecord(), null));
		assertNull(new ImpulseAggregate().aggregate(null, new MACDHistogramRecord()));
		assertNull(new ImpulseAggregate().aggregate(new DoubleRecord(), new MACDHistogramRecord()));
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
	
	private DoubleRecord emaData(double vChange) {
		return DoubleRecord.builder()
				.vChange(vChange)
				.build();
	}
	
	private MACDHistogramRecord macdData(double hChange) {
		return MACDHistogramRecord.builder()
				.hChange(hChange)
				.build();
	}
}
