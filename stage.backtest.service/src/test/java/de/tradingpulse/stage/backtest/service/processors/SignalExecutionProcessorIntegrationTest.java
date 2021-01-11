package de.tradingpulse.stage.backtest.service.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TimeRange;
import de.tradingpulse.common.utils.TimeUtils;
import de.tradingpulse.stage.backtest.recordtypes.SignalExecutionRecord;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.test.kafka.AbstractTopologyTest;
import de.tradingpulse.streams.test.kafka.TopologyTestContext;

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
	void test_ENTRY_EXIT() throws InterruptedException {
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

	@Test
	void test_ENTRY_ONGOING_EXIT() throws InterruptedException {
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
		SignalRecord record = SignalRecord.builder()
				.key(new SymbolTimestampKey(SYMBOL, timestamp))
				.timeRange(TimeRange.DAY)
				.strategyKey(STRATEGY_KEY)
				.signalType(signalType)
				.build();
		
		this.signalDailyTopic.pipeInput(record.getKey(), record);
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
				
		this.ohlcvDailyTopic.pipeInput(record.getKey(), record);
	}

}
