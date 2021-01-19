package de.tradingpulse.stage.tradingscreens.service.processors;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.NEUTRAL;
import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TimeRange;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.common.utils.TimeUtils;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType;
import de.tradingpulse.stage.tradingscreens.recordtypes.SwingSignalType;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.test.kafka.AbstractTopologyTest;
import de.tradingpulse.streams.test.kafka.TopologyTestContext;

class SignalsProcessorIntegrationTest extends AbstractTopologyTest {
	
	private TestInputTopic<SymbolTimestampKey, ImpulseTradingScreenRecord> impulseTradingScreenTopic;
	private TestOutputTopic<SymbolTimestampKey, SignalRecord> signalDailyTopic;

	@Override
	protected void initTopology(TopologyTestContext topologyTestContext) {
		createSignalProcessor(topologyTestContext)
		.createTopology(createImpulseTradingScreenStream(topologyTestContext));
	}

	private SignalsProcessor createSignalProcessor(TopologyTestContext topologyTestContext) {
		SignalsProcessor p = new SignalsProcessor();
		p.streamsBuilder = topologyTestContext.getStreamsBuilder();
		p.jsonSerdeRegistry = topologyTestContext.getJsonSerdeRegistry();
		
		return p;
	}

	private KStream<SymbolTimestampKey, ImpulseTradingScreenRecord> createImpulseTradingScreenStream(
			TopologyTestContext topologyTestContext)
	{
		return topologyTestContext.createKStream(
				TradingScreensStreamsFacade.TOPIC_IMPULSE_TRADING_SCREEN, 
				SymbolTimestampKey.class, 
				ImpulseTradingScreenRecord.class);
	}

	@Override
	protected void initTestTopics(TopologyTestContext topologyTestContext) {
		this.impulseTradingScreenTopic = topologyTestContext.createTestInputTopic(
				TradingScreensStreamsFacade.TOPIC_IMPULSE_TRADING_SCREEN, 
				SymbolTimestampKey.class, 
				ImpulseTradingScreenRecord.class);
		
		this.signalDailyTopic = topologyTestContext.createTestOutputTopic(
				TradingScreensStreamsFacade.TOPIC_SIGNAL_DAILY, 
				SymbolTimestampKey.class, 
				SignalRecord.class);
	}

	// ------------------------------------------------------------------------
	// tests
	// ------------------------------------------------------------------------

	@Test
	void test_ENTRY_LONG_POTENTIAL_followed_by_ENTRY_SHORT_POTENTIAL() {

		// when on first day
		long firstDay = TimeUtils.getTimestampDaysBeforeNow(7);
		sendImpulseTradingScreenRecord(firstDay, NEUTRAL, NEUTRAL, SHORT, NEUTRAL);
		
		// and on second day
		long secondDay = firstDay + ONE_DAY;
		sendImpulseTradingScreenRecord(secondDay, NEUTRAL, SHORT, NEUTRAL, NEUTRAL);
		
		// then
		assertEquals(4, this.signalDailyTopic.getQueueSize());
		
		// and
		SignalRecord r = this.signalDailyTopic.readValue();
		assertEquals(firstDay, r.getTimeRangeTimestamp());
		assertEquals(SwingSignalType.SWING_MARKET_TURN_POTENTIAL.name(), r.getStrategyKey());
		assertEquals(SignalType.ENTRY_LONG, r.getSignalType());
		
		// and
		r = this.signalDailyTopic.readValue();
		assertEquals(secondDay, r.getTimeRangeTimestamp());
		assertEquals(SwingSignalType.SWING_MOMENTUM.name(), r.getStrategyKey());
		assertEquals(SignalType.EXIT_LONG, r.getSignalType());
		
		// and
		r = this.signalDailyTopic.readValue();
		assertEquals(secondDay, r.getTimeRangeTimestamp());
		assertEquals(SwingSignalType.SWING_MARKET_TURN_POTENTIAL.name(), r.getStrategyKey());
		assertEquals(SignalType.EXIT_LONG, r.getSignalType());
		
		// and
		r = this.signalDailyTopic.readValue();
		assertEquals(secondDay, r.getTimeRangeTimestamp());
		assertEquals(SwingSignalType.SWING_MARKET_TURN_POTENTIAL.name(), r.getStrategyKey());
		assertEquals(SignalType.ENTRY_SHORT, r.getSignalType());
	}

	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------

	private void sendImpulseTradingScreenRecord(
			long timestamp, 
			TradingDirection longLast,
			TradingDirection longNow,
			TradingDirection shortLast,
			TradingDirection shortNow)
	{
		ImpulseTradingScreenRecord r = createImpulseTradingScreenRecord(timestamp, longLast, longNow, shortLast, shortNow);
		this.impulseTradingScreenTopic.pipeInput(r.getKey(), r);
	}
	
	private ImpulseTradingScreenRecord createImpulseTradingScreenRecord(
			long timestamp, 
			TradingDirection longLast,
			TradingDirection longNow,
			TradingDirection shortLast,
			TradingDirection shortNow)
	{		
		return ImpulseTradingScreenRecord.builder()
				.key(new SymbolTimestampKey(SYMBOL, timestamp))
				.timeRange(TimeRange.DAY)
				.longRangeImpulseRecord(
						createImpulseRecord(
								TimeUtils.getStartOfWeekTimestampUTC(timestamp), 
								TimeRange.WEEK, 
								longLast, 
								longNow))
				.shortRangeImpulseRecord(
						createImpulseRecord(
								timestamp, 
								TimeRange.DAY, 
								shortLast, 
								shortNow))
				.build();
				
				
	}
	
	private ImpulseRecord createImpulseRecord(long timestamp, TimeRange timeRange, TradingDirection last, TradingDirection now) {
		return ImpulseRecord.builder()
				.key(new SymbolTimestampKey(SYMBOL, timestamp))
				.timeRange(timeRange)
				.lastTradingDirection(last)
				.tradingDirection(now)
				.build();
	}
}
