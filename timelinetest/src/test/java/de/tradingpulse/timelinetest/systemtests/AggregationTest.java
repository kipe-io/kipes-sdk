package de.tradingpulse.timelinetest.systemtests;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.PrimitiveIterator.OfDouble;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import ch.qos.logback.core.util.TimeUtil;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.utils.TimeUtils;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.timelinetest.Consumer;
import de.tradingpulse.timelinetest.Producer;
import de.tradingpulse.timelinetest.streams.MinuteStreamFactory;
import de.tradingpulse.timelinetest.streams.SecondsStreamsFactory;
import io.micronaut.test.annotation.MicronautTest;

@MicronautTest
@TestInstance(Lifecycle.PER_CLASS)
class AggregationTest {

	private OfDouble d = new Random().doubles(10.0, 20.0).iterator();
	
	@Inject @Named(SecondsStreamsFactory.TOPIC_DATA_15_SEC)
	private KStream<SymbolTimestampKey, OHLCVRecord> data15Sec;

	@Inject @Named(MinuteStreamFactory.TOPIC_DATA_AGG_MINUTE)
	private KStream<SymbolTimestampKey, OHLCVRecord> dataAggMinute;

	@Inject @Named(MinuteStreamFactory.TOPIC_DATA_WINDOWED_MINUTE)
	private KStream<SymbolTimestampKey, OHLCVRecord> dataWinMinute;

	@Inject
	private Producer producer;
	
	@Inject 
	private Consumer consumer;
	
	@BeforeEach
	void beforeEach() throws InterruptedException, ExecutionException {
		this.consumer.setReceivedDataAggMin(null);
		this.consumer.setReceivedDataWinMin(null);
	}
	
	@Test
	@Order(1)
	void test__verify_aggregation() {
		long ts = TimeUtil.computeStartOfNextMinute(System.currentTimeMillis());
		
		await().atMost(Duration.ofMinutes(1)).until(() -> System.currentTimeMillis() >= ts);
		
		OHLCVRecord data1 = generateOHLCVData();		
		producer.send(data1.getKey(), data1);

		OHLCVRecord data2 = generateOHLCVData();
		producer.send(data2.getKey(), data2);

		OHLCVRecord data3 = generateOHLCVData();
		producer.send(data3.getKey(), data3);
		
		await().atMost(Duration.ofMinutes(1)).until(() -> consumer.getReceivedDataAggMin() != null );
		
		OHLCVRecord dataAgg = data1.aggregateWith(data2).aggregateWith(data3);
		dataAgg.getKey().setTimestamp(TimeUtils.getStartOfMinuteTimestampUTC(dataAgg.getKey().getTimestamp()));
		assertEquals(dataAgg, consumer.getReceivedDataAggMin());
		assertEquals(dataAgg, consumer.getReceivedDataWinMin());
	}
	
	private OHLCVRecord generateOHLCVData() {
		return OHLCVRecord.builder()
				.key(SymbolTimestampKey.builder()
						.symbol("symbol")
						.timestamp(System.currentTimeMillis())
						.build())
				.open(d.next())
				.high(d.next())
				.low(d.next())
				.close(d.next())
				.volume(d.next().longValue())
				.build();
	}
}
