package de.tradingpulse.stage.backtest.service.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import io.micronaut.configuration.kafka.serde.JsonSerde;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.BeanContext;
import io.micronaut.core.reflect.ClassUtils;

class SignalExecutionProcessorIntegrationTest {
	
	private static final long ONE_DAY = 86400000L;
	
	private static final String STRATEGY_KEY = "strategyKey";
	private static final String SYMBOL = "symbol";

	private static final Properties CONFIG = new Properties();
	static {
		CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
	}
	
	private final JsonSerdeRegistry jsonSerdeRegistry = MockedJsonSerdeRegistry.create();
	
	// @BeforeEach initializes the following members
	private ConfiguredStreamBuilder streamBuilder;
	private KStream<SymbolTimestampKey, SignalRecord> signalDailyStream;
	private KStream<SymbolTimestampKey, OHLCVRecord> ohlcvDailyStream;
	private TopologyTestDriver driver;
	
	private TestInputTopic<SymbolTimestampKey, SignalRecord> signalDailyTopic;
	private TestInputTopic<SymbolTimestampKey, OHLCVRecord> ohlcvDailyTopic;
	private TestOutputTopic<SymbolTimestampKey, SignalExecutionRecord> signalExecutionTopic;
	
	@BeforeEach
	void beforeEach() {
		this.streamBuilder = new ConfiguredStreamBuilder(CONFIG);
		this.signalDailyStream = createSignalDailyStream();
		this.ohlcvDailyStream = createOhlcvDailyStream();
		
		this.driver = new TopologyTestDriver(buildTopology(), CONFIG);
		
		this.signalDailyTopic = createTestInputTopic(
				TradingScreensStreamsFacade.TOPIC_SIGNAL_DAILY, 
				SymbolTimestampKey.class, 
				SignalRecord.class);
		
		this.ohlcvDailyTopic = createTestInputTopic(
				SourceDataStreamsFacade.TOPIC_OHLCV_DAILY, 
				SymbolTimestampKey.class, 
				OHLCVRecord.class);
		
		this.signalExecutionTopic = createTestOutputTopic(
				BacktestStreamsFacade.TOPIC_SIGNAL_EXECUTION_DAILY, 
				SymbolTimestampKey.class, 
				SignalExecutionRecord.class);
	}
	
	@AfterEach
	void afterEach() {
		this.driver.close();
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
	
	
	private <K,V> TestInputTopic<K,V> createTestInputTopic(String topic, Class<K> keyType, Class<V> valueType) {
		return this.driver.createInputTopic(
				topic, 
				jsonSerdeRegistry.getSerializer(keyType), 
				jsonSerdeRegistry.getSerializer(valueType));
	}
	
	private <K,V> TestOutputTopic<K, V> createTestOutputTopic(String topic, Class<K> keyType, Class<V> valueType) {
		return this.driver.createOutputTopic(
				topic, 
				jsonSerdeRegistry.getDeserializer(keyType), 
				jsonSerdeRegistry.getDeserializer(valueType));
	}
	
	private Topology buildTopology() {
		createSignalExecutionProcessor()
		.createTopology(
				signalDailyStream, 
				ohlcvDailyStream);
		
		return streamBuilder.build();
	}
	
	/**
	 * <pre>
	 * Creates an SignalExecutionProcessor with
	 * - streamBuilder
	 * - jsonSerdeRegistry
	 * initialized.  
	 * </pre> 
	 */
	private SignalExecutionProcessor createSignalExecutionProcessor() {
		SignalExecutionProcessor p = new SignalExecutionProcessor();
		p.streamBuilder = this.streamBuilder;
		p.jsonSerdeRegistry = this.jsonSerdeRegistry;
		
		return p;
	}
    
	private KStream<SymbolTimestampKey, SignalRecord> createSignalDailyStream() {
		
		return streamBuilder
				.stream(TradingScreensStreamsFacade.TOPIC_SIGNAL_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(SignalRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
    }
	
    KStream<SymbolTimestampKey, OHLCVRecord> createOhlcvDailyStream() {
		
		return streamBuilder
				.stream(SourceDataStreamsFacade.TOPIC_OHLCV_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(OHLCVRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
    }
    
	// ------------------------------------------------------------------------
	// MockedJsonSerdeRegistry
	// ------------------------------------------------------------------------

	private static class MockedJsonSerdeRegistry extends JsonSerdeRegistry {
		static JsonSerdeRegistry create() {
			return new MockedJsonSerdeRegistry(mock(BeanContext.class));
		}
		
		private final BeanContext beanContextMock;
		
		public MockedJsonSerdeRegistry(BeanContext mock) {
			super(mock);
			
			this.beanContextMock = mock;
		}

		@Override
		public <T> Serde<T> getSerde(Class<T> type) {
			if(ClassUtils.isJavaBasicType(type)) {
				return super.getSerde(type);
			}
			reset(beanContextMock);
			
			lenient()
			.when(beanContextMock.createBean(JsonSerde.class, type))
			.thenReturn(new JsonSerde<>(type));
			
			return super.getSerde(type);
		}
		
	}
}
