package de.tradingpulse.stage.backtest.service.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TimeRange;
import de.tradingpulse.common.utils.TimeUtils;
import de.tradingpulse.stage.backtest.recordtypes.BacktestResultRecord;
import de.tradingpulse.stage.backtest.recordtypes.SignalExecutionRecord;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType;

class BacktestResultProcessorIntegrationTest extends AbstractTopologyTest {

	private TestInputTopic<SymbolTimestampKey, SignalExecutionRecord> signalExecutionDailyTopic;
	private TestOutputTopic<SymbolTimestampKey, BacktestResultRecord> backtestResultDailyTopic;

	@Override
	protected void initTopology(TopologyTestContext topologyTestContext) {
		createBacktestResultProcessor(topologyTestContext)
		.createTopology(createSignalExecutionDailyStream(topologyTestContext));
		
	}

	private BacktestResultProcessor createBacktestResultProcessor(TopologyTestContext topologyTestContext) {
		BacktestResultProcessor p = new BacktestResultProcessor();
		p.streamBuilder = topologyTestContext.getStreamsBuilder();
		p.jsonSerdeRegistry = topologyTestContext.getJsonSerdeRegistry();
		
		return p;
	}

	private KStream<SymbolTimestampKey, SignalExecutionRecord> createSignalExecutionDailyStream(TopologyTestContext topologyTestContext) {
		return topologyTestContext.createKStream(
				BacktestStreamsFacade.TOPIC_SIGNAL_EXECUTION_DAILY, 
				SymbolTimestampKey.class, 
				SignalExecutionRecord.class);
	}

	@Override
	protected void initTestTopics(TopologyTestContext topologyTestContext) {
		this.signalExecutionDailyTopic = topologyTestContext.createTestInputTopic(
				BacktestStreamsFacade.TOPIC_SIGNAL_EXECUTION_DAILY, 
				SymbolTimestampKey.class, 
				SignalExecutionRecord.class);
		
		this.backtestResultDailyTopic = topologyTestContext.createTestOutputTopic(
				BacktestStreamsFacade.TOPIC_BACKTESTRESULT_DAILY, 
				SymbolTimestampKey.class, 
				BacktestResultRecord.class);		
	}
	
	// ------------------------------------------------------------------------
	// tests
	// ------------------------------------------------------------------------

	@Test
	void test_ENTRY_ONGOING_EXIT() {
		// when on first day
		long firstDay = TimeUtils.getTimestampDaysBeforeNow(7);
		sendSignalExecution(firstDay, SignalType.ENTRY_LONG, 1.0, 0.5);
		
		// and on second day
		long secondDay = firstDay + ONE_DAY;
		sendSignalExecution(secondDay, SignalType.ONGOING_LONG, 2.0, 3.5);
		
		// and on third day
		long thirdDay = secondDay + ONE_DAY;
		sendSignalExecution(thirdDay, SignalType.EXIT_LONG, 3.0, 2.0);
		
		// then
		BacktestResultRecord r = this.backtestResultDailyTopic.readValue();
		assertEquals(firstDay, r.getEntryTimestamp());
		assertEquals(thirdDay, r.getTimeRangeTimestamp());
		assertEquals(1.0, r.getEntry());
		assertEquals(3.5, r.getHigh());
		assertEquals(0.5, r.getLow());
		assertEquals(3.0, r.getExit());
		
		// and no more records
		this.backtestResultDailyTopic.isEmpty();
	}
	
	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------
	
	private void sendSignalExecution(long timestamp, SignalType signalType, double entryExit, double highLow) {
		
		SignalExecutionRecord record = SignalExecutionRecord.builder()
				.key(new SymbolTimestampKey(SYMBOL, timestamp))
				.timeRange(TimeRange.DAY)
				.signalRecord(SignalRecord.builder()
						.key(new SymbolTimestampKey(SYMBOL, timestamp - ONE_DAY)) // signals happen always one timeRange earlier
						.timeRange(TimeRange.DAY)
						.strategyKey(STRATEGY_KEY)
						.signalType(signalType)
						.build())
				.ohlcvRecord(OHLCVRecord.builder()
						.key(new SymbolTimestampKey(SYMBOL, timestamp))
						.timeRange(TimeRange.DAY)
						.open(entryExit)
						.high(Math.max(entryExit, highLow))
						.low(Math.min(entryExit, highLow))
						.close(entryExit)
						.volume(1L)
						.build())
				.build();
		
		this.signalExecutionDailyTopic.pipeInput(record.getKey(), record);
	}

}
