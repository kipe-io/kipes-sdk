package de.tradingpulse.stage.backtest.service.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TimeRange;
import de.tradingpulse.stage.backtest.recordtypes.SignalExecutionRecord;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import io.kipe.common.utils.TimeUtils;
import io.kipe.streams.test.kafka.AbstractTopologyTest;
import io.kipe.streams.test.kafka.TopologyTestContext;

class SignalExecutionProcessorIntegrationTest extends AbstractTopologyTest{
	
	private TestInputTopic<SymbolTimestampKey, SignalRecord> signalDailyTopic;
	private TestInputTopic<SymbolTimestampKey, OHLCVRecord> ohlcvDailyTopic;
	private TestOutputTopic<SymbolTimestampKey, SignalExecutionRecord> signalExecutionTopic;
	
	@Override
	protected void initTopology(TopologyTestContext topologyTestContext) {
		createSignalExecutionProcessor(topologyTestContext)
		.createTopology(
				createSignalDailyStream(topologyTestContext), 
				createOhlcvDailyStream(topologyTestContext));
	}
	
	private SignalExecutionProcessor createSignalExecutionProcessor(TopologyTestContext topologyTestContext) {
		SignalExecutionProcessor p = new SignalExecutionProcessor();
		p.streamBuilder = topologyTestContext.getStreamsBuilder();
		p.jsonSerdeRegistry = topologyTestContext.getJsonSerdeRegistry();
		
		return p;
	}
    
	private KStream<SymbolTimestampKey, SignalRecord> createSignalDailyStream(TopologyTestContext topologyTestContext) {
		
		return topologyTestContext.createKStream(
				TradingScreensStreamsFacade.TOPIC_SIGNAL_DAILY, 
				SymbolTimestampKey.class, 
				SignalRecord.class);
    }
	
    private KStream<SymbolTimestampKey, OHLCVRecord> createOhlcvDailyStream(TopologyTestContext topologyTestContext) {
		
		return topologyTestContext.createKStream(
				SourceDataStreamsFacade.TOPIC_OHLCV_DAILY, 
				SymbolTimestampKey.class, 
				OHLCVRecord.class);
    }

	@Override
	protected void initTestTopics(TopologyTestContext topologyTestContext) {
		this.signalDailyTopic = topologyTestContext.createTestInputTopic(
				TradingScreensStreamsFacade.TOPIC_SIGNAL_DAILY, 
				SymbolTimestampKey.class, 
				SignalRecord.class);
		
		this.ohlcvDailyTopic = topologyTestContext.createTestInputTopic(
				SourceDataStreamsFacade.TOPIC_OHLCV_DAILY, 
				SymbolTimestampKey.class, 
				OHLCVRecord.class);
		
		this.signalExecutionTopic = topologyTestContext.createTestOutputTopic(
				BacktestStreamsFacade.TOPIC_SIGNAL_EXECUTION_DAILY, 
				SymbolTimestampKey.class, 
				SignalExecutionRecord.class);		
	}
		
	// ------------------------------------------------------------------------
	// tests
	// ------------------------------------------------------------------------

	@Test
	void testBUG_TSLA_EXIT_on_friday_gets_ignored() {
		// we got an exit on friday which gets ignored
		
		// given ohlcv records starting on
		long first_day_2020_04_01 = 1585699200000L;
		sendOHLCV(first_day_2020_04_01,              1.0);	// WED,  1st
		sendOHLCV(first_day_2020_04_01 +  1*ONE_DAY, 1.0);	// THU,  2nd
		sendOHLCV(first_day_2020_04_01 +  2*ONE_DAY, 1.0);	// FRI,  3rd
		sendOHLCV(first_day_2020_04_01 +  5*ONE_DAY, 1.0);	// MON,  6th
		sendOHLCV(first_day_2020_04_01 +  6*ONE_DAY, 1.0);	// TUE,  7th
		
		// and following signals
		sendSignal(first_day_2020_04_01,              "SWING_MOMENTUM",              SignalType.ENTRY_SHORT);	// WED,  1st
		sendSignal(first_day_2020_04_01,              "SWING_MARKET_TURN_POTENTIAL", SignalType.ENTRY_SHORT);	// WED,  1st
		sendSignal(first_day_2020_04_01 +  2*ONE_DAY, "SWING_MOMENTUM",              SignalType.EXIT_SHORT);	// FRI,  3rd
		sendSignal(first_day_2020_04_01 +  5*ONE_DAY, "SWING_MARKET_TURN_POTENTIAL", SignalType.EXIT_SHORT);	// MON,  6th
		
		// then we should get plenty of signal executions
		//
		// SWING_MOMENTUM
		// --------------
		// 2020-04-02 ENTRY_SHORT
		// 2020-04-03 ONGOING_SHORT
		// 2020-04-06 EXIT_SHORT
		// 
		// SWING_MARKET_TURN_POTENTIAL
		// ---------------------------
		// 2020-04-02 ENTRY_SHORT
		// 2020-04-03 ONGOING_SHORT
		// 2020-04-04 ONGOING_SHORT - SAT
		// 2020-04-05 ONGOING_SHORT - SUN
		// 2020-04-06 ONGOING_SHORT
		// 2020-04-07 EXIT_SHORT
		
		assertEquals(9, this.signalExecutionTopic.getQueueSize());		
	}
	
	//@Test
	void testBUG_TPR_first_signals() {
		// we have two exit signals followed by an entry which doesn't trigger
		// an ongoing signal execution
		
		// given ohlcv records starting on 2019-07-23 (1563840000000)
		long first_day_2019_07_23 = 1563840000000L;
		sendOHLCV(first_day_2019_07_23,              1.0);	// TUE, 23th
		sendOHLCV(first_day_2019_07_23 +  1*ONE_DAY, 1.0);	// WED, 24th
		sendOHLCV(first_day_2019_07_23 +  2*ONE_DAY, 1.0);	// THU, 25th
		sendOHLCV(first_day_2019_07_23 +  3*ONE_DAY, 1.0);	// FRI, 26th
		sendOHLCV(first_day_2019_07_23 +  6*ONE_DAY, 1.0);	// MON, 29th
		sendOHLCV(first_day_2019_07_23 +  7*ONE_DAY, 1.0);	// TUE, 30th
		sendOHLCV(first_day_2019_07_23 +  8*ONE_DAY, 1.0);	// WED, 31th
		sendOHLCV(first_day_2019_07_23 +  9*ONE_DAY, 1.0);	// THU,  1st
		sendOHLCV(first_day_2019_07_23 + 10*ONE_DAY, 1.0);	// FRI,  2nd
		
		// and following signals
		sendSignal(first_day_2019_07_23,              "SWING_MOMENTUM",              SignalType.EXIT_SHORT);	// TUE, 23th
		sendSignal(first_day_2019_07_23 +  1*ONE_DAY, "SWING_MARKET_TURN_POTENTIAL", SignalType.EXIT_SHORT);	// WED, 24th
		sendSignal(first_day_2019_07_23 +  1*ONE_DAY, "SWING_MARKET_TURN_POTENTIAL", SignalType.ENTRY_LONG);	// WED, 24th
		sendSignal(first_day_2019_07_23 +  8*ONE_DAY, "SWING_MOMENTUM",              SignalType.ENTRY_LONG);	// WED, 31th
		sendSignal(first_day_2019_07_23 +  9*ONE_DAY, "SWING_MOMENTUM",              SignalType.EXIT_LONG);		// THU,  1st
		sendSignal(first_day_2019_07_23 +  9*ONE_DAY, "SWING_MARKET_TURN_POTENTIAL", SignalType.EXIT_LONG);		// THU,  1st
		sendSignal(first_day_2019_07_23 +  9*ONE_DAY, "SWING_MOMENTUM",              SignalType.ENTRY_SHORT);	// THU,  1st
		sendSignal(first_day_2019_07_23 +  9*ONE_DAY, "SWING_MARKET_TURN_POTENTIAL", SignalType.ENTRY_SHORT);	// THU,  1st
		
		// then we should get plenty of signal executions:
		//
		// SWING_MARKET_TURN_POTENTIAL
		// ---------------------------
		// 2019-07-25 ENTRY_LONG
		// 2019-07-26 ONGOING_LONG
		// 2019-07-27 ONGOING_LONG - SAT
		// 2019-07-28 ONGOING_LONG - SUN
		// 2019-07-29 ONGOING_LONG
		// 2019-07-30 ONGOING_LONG
		// 2019-07-31 ONGOING_LONG
		// 2019-08-01 ONGOING_LONG
		// 2019-08-02 EXIT_LONG
		//
		// SWING_MOMENTUM
		// --------------
		// 2019-08-01 ENTRY_LONG
		// 2019-08-02 EXIT_LONG
		
		assertEquals(11, this.signalExecutionTopic.getQueueSize());
	}
	
	//@Test
	void test_ENTRY_EXIT() {
		// when on first day
		long firstDay = TimeUtils.getTimestampDaysBeforeNow(7);
		sendOHLCV(firstDay, 1.0);
		sendSignal(firstDay, SignalType.ENTRY_LONG);
		
		// and on second day
		long secondDay = firstDay + ONE_DAY;
		sendOHLCV(secondDay, 2.0);
		sendSignal(secondDay, SignalType.EXIT_LONG);
		
		// and on third day
		long thirdDay = secondDay + ONE_DAY;
		sendOHLCV(thirdDay, 3.0);
		
		// then
		assertEquals(2, this.signalExecutionTopic.getQueueSize());
		
		// and entry execution is on second day
		SignalExecutionRecord ser = this.signalExecutionTopic.readValue();
		assertEquals(secondDay, ser.getTimeRangeTimestamp());
		assertEquals(SignalType.ENTRY_LONG, ser.getSignalRecord().getSignalType());
		
		assertEquals(firstDay, ser.getSignalRecord().getTimeRangeTimestamp());
		assertEquals(secondDay, ser.getOhlcvRecord().getTimeRangeTimestamp());
		assertEquals(2.0, ser.getOhlcvRecord().getOpen());
		
		// and exit execution is on third day
		ser = this.signalExecutionTopic.readValue();
		assertEquals(thirdDay, ser.getTimeRangeTimestamp());
		assertEquals(SignalType.EXIT_LONG, ser.getSignalRecord().getSignalType());
		assertEquals(secondDay, ser.getSignalRecord().getTimeRangeTimestamp());
		assertEquals(thirdDay, ser.getOhlcvRecord().getTimeRangeTimestamp());
		assertEquals(3.0, ser.getOhlcvRecord().getOpen());
		
		// and there are no more records
		assertTrue(this.signalExecutionTopic.isEmpty());
	}

	//@Test
	void test_ENTRY_ONGOING_EXIT()  {
		// when on first day
		long firstDay = TimeUtils.getTimestampDaysBeforeNow(7);
		sendOHLCV(firstDay, 1.0);
		sendSignal(firstDay, SignalType.ENTRY_SHORT);
		
		// and on second day
		long secondDay = firstDay + ONE_DAY;
		sendOHLCV(secondDay, 2.0);
		
		// and on third day
		long thirdDay = secondDay + ONE_DAY;
		sendOHLCV(thirdDay, 3.0);
		sendSignal(thirdDay, SignalType.EXIT_SHORT);
		
		// and on fourth day
		long fourthDay = thirdDay + ONE_DAY;
		sendOHLCV(fourthDay, 4.0);
		
		// then
		assertEquals(3, this.signalExecutionTopic.getQueueSize());
		
		// and entry execution is on second day
		SignalExecutionRecord ser = this.signalExecutionTopic.readValue();
		assertEquals(secondDay, ser.getTimeRangeTimestamp());
		assertEquals(SignalType.ENTRY_SHORT, ser.getSignalRecord().getSignalType());
		
		assertEquals(firstDay, ser.getSignalRecord().getTimeRangeTimestamp());
		assertEquals(secondDay, ser.getOhlcvRecord().getTimeRangeTimestamp());
		assertEquals(2.0, ser.getOhlcvRecord().getOpen());
		
		// and ongoing execution on third day
		ser = this.signalExecutionTopic.readValue();
		assertEquals(thirdDay, ser.getTimeRangeTimestamp());
		assertEquals(SignalType.ONGOING_SHORT, ser.getSignalRecord().getSignalType());
		
		assertEquals(secondDay, ser.getSignalRecord().getTimeRangeTimestamp());
		assertEquals(thirdDay, ser.getOhlcvRecord().getTimeRangeTimestamp());
		assertEquals(3.0, ser.getOhlcvRecord().getOpen());
		
		// and exit execution is on fourth day
		ser = this.signalExecutionTopic.readValue();
		assertEquals(fourthDay, ser.getTimeRangeTimestamp());
		assertEquals(SignalType.EXIT_SHORT, ser.getSignalRecord().getSignalType());
		
		assertEquals(thirdDay, ser.getSignalRecord().getTimeRangeTimestamp());
		assertEquals(fourthDay, ser.getOhlcvRecord().getTimeRangeTimestamp());
		assertEquals(4.0, ser.getOhlcvRecord().getOpen());
		
		// and there are no more records
		assertTrue(this.signalExecutionTopic.isEmpty());
	}
	
	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------
	
	private void sendSignal(long timestamp, SignalType signalType) {
		sendSignal(timestamp, STRATEGY_KEY, signalType);
	}
	
	private void sendSignal(long timestamp, String strategyKey, SignalType signalType) {
		SignalRecord record = SignalRecord.builder()
				.key(new SymbolTimestampKey(SYMBOL, timestamp))
				.timeRange(TimeRange.DAY)
				.strategyKey(strategyKey)
				.signalType(signalType)
				.build();
		
		this.signalDailyTopic.pipeInput(record.getKey(), record, Instant.ofEpochMilli(timestamp));
	}
	
	private void sendOHLCV(long timestamp, double value) {
		OHLCVRecord record = OHLCVRecord.builder()
				.key(new SymbolTimestampKey(SYMBOL, timestamp))
				.timeRange(TimeRange.DAY)
				.open(value)
				.high(value)
				.low(value)
				.close(value)
				.volume(1L)
				.build();
				
		this.ohlcvDailyTopic.pipeInput(record.getKey(), record, Instant.ofEpochMilli(timestamp));
	}

}
