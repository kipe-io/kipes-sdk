package de.tradingpulse.stage.systems.aggregates;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.LONG;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
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
		assertEquals(LONG, new ImpulseAggregate().aggregate(emaRecord(1.0), macdRecord(1.0)).getTradingDirection());
	}

	@Test
	void test_aggregate__calculates_correctly_short() {
		assertEquals(SHORT, new ImpulseAggregate().aggregate(emaRecord(-1.0), macdRecord(-1.0)).getTradingDirection());
	}

	@Test
	void test_aggregate__calculates_correctly_neutral() {
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaRecord( 0.0), macdRecord( 0.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaRecord( 0.0), macdRecord(-1.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaRecord( 0.0), macdRecord( 1.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaRecord(-1.0), macdRecord( 0.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaRecord( 1.0), macdRecord( 0.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaRecord(-1.0), macdRecord( 1.0)).getTradingDirection());
		assertEquals(NEUTRAL, new ImpulseAggregate().aggregate(emaRecord( 1.0), macdRecord(-1.0)).getTradingDirection());
	}

	@Test
	void test_aggregate__calculates_correctly_lastDirection_also_with_serde()
	throws JsonProcessingException
	{
		TradingDirection lastDirection = NEUTRAL;
		
		ImpulseRecord impulseRecord = ImpulseRecord.builder()
				.key(SymbolTimestampKey.builder()
						.symbol("symbol")
						.timestamp(0)
						.build())
				.lastTradingDirection(SHORT)
				.tradingDirection(lastDirection)
				.build();
		
		ImpulseAggregate a = new ImpulseAggregate(impulseRecord);
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(a);
		a = mapper.readValue(json, ImpulseAggregate.class);
		
		assertEquals(lastDirection, a.aggregate(emaRecord(1.0), macdRecord(1.0)).getLastTradingDirection());
	}
	
	private DoubleRecord emaRecord(double vChange) {
		return DoubleRecord.builder()
				.vChange(vChange)
				.build();
	}
	
	private MACDHistogramRecord macdRecord(double hChange) {
		return MACDHistogramRecord.builder()
				.hChange(hChange)
				.build();
	}
}
