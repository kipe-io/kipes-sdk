package de.tradingpulse.stage.tradingscreens.service.processors;

import java.time.Duration;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.common.utils.MathUtils;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class TrendsValueProcessor extends AbstractProcessorFactory {
	
	@Inject
	SystemsStreamsFacade systemsStreamFacade;
	
	@Inject
	ConfiguredStreamBuilder streamBuilder;

	@Inject
	JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() throws Exception {
		createTopology(
				TradingScreensStreamsFacade.TOPIC_TRENDS_VALUE, 
				systemsStreamFacade.getTrendsDailyStream(), 
				systemsStreamFacade.getTrendsWeeklyStream());
	}
	
	void createTopology(
			String topic,
			KStream<SymbolTimestampKey, GenericRecord> shortRangeTrendsStream,
			KStream<SymbolTimestampKey, GenericRecord> longRangeTrendsStream)
	{
		// --------------------------------------------------------------------
		// from
		//   shortRangeTrendsStream
		//
		// transform
		//   as
		//		GenericRecord
		//			with trendSummaryValue = ...
		//
		// to
		//   shortRangeTrendsValue
		//
		// --------------------------------------------------------------------
		// from
		//   longRangeTrendsStream
		//
		// transform
		//   as
		//		GenericRecord
		//			with trendSummaryValue = ...
		//
		// to
		//   longRangeTrendsValue
		//
		// --------------------------------------------------------------------
		// from
		//   shortRangeTrendsValue
		//
		// join
		//   trendSummaryWeekly
		//   windowSize 0
		//   retentionPeriod 2 years 1 day
		//   as
		//     GenericRecord
		//       with shortTrendSummaryValue = ... 
		//       with longTtrendSummaryValue = ... 
		//
		// to
		//   topic
		// --------------------------------------------------------------------
		
		KStream<SymbolTimestampKey, GenericRecord> shortRangeTrendsValueStream = createTrendsValueStream(
				topic+"_short_range_trends_value", "shortRange", 
				shortRangeTrendsStream);
		
		KStream<SymbolTimestampKey, GenericRecord> longRangeTrendsValueStream = createTrendsValueStream(
				topic+"_long_range_trends_value", "longRange", 
				longRangeTrendsStream);
		
		TopologyBuilder
		.init(streamBuilder)
		
		.from(
				shortRangeTrendsValueStream, 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.withTopicsBaseName(topic)
		
		.<GenericRecord, GenericRecord> join(
				longRangeTrendsValueStream,
				jsonSerdeRegistry.getSerde(GenericRecord.class))
				
			.withWindowSize(Duration.ZERO)
			.withRetentionPeriod(Duration.ofMillis(this.retentionMs + 86400000L))
			.as(
					(shortRange, longRange) -> shortRange.copy()
						.withNewFieldsFrom(longRange),
						jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.to(topic);
	}
	
	KStream<SymbolTimestampKey, GenericRecord> createTrendsValueStream(
			String topicsBaseName,
			String prefix,
			KStream<SymbolTimestampKey, GenericRecord> trendsStream)
	{
		// --------------------------------------------------------------------
		// from
		//   trendsDailyStream
		//
		// transform
		//   as
		//		GenericRecord
		//			with trendSummaryValue = ...
		//
		// stream
		// --------------------------------------------------------------------

		return TopologyBuilder
		.init(streamBuilder)
		
		.from(
				trendsStream, 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.withTopicsBaseName(topicsBaseName)
		
		.<SymbolTimestampKey, GenericRecord> transform()
			.changeValue(
					(key, trends) -> GenericRecord.create()
						.with("key", trends.get("key"))
						.with(prefix+"TrendsValue", evaluateTrendsValue(trends)))
			.asValueType(
					jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.getStream();
	}

	private Double evaluateTrendsValue(GenericRecord trends) {
		double sum = 0.0;
		sum += getTrendValue(trends.getString("trendEMA"));
		sum += getTrendValue(trends.getString("trendMacdHistogram"));
		sum += getTrendValue(trends.getString("trendMACDLinesSlope"));
		sum += getTrendValue(trends.getString("trendMACDValue"));
		sum += getTrendValue(trends.getString("trendSSTOCSlope"));
		sum += getTrendValue(trends.getString("trendSSTOCValue"));
		
		return MathUtils.round(sum / 6.0, 2);
	}
	
	private double getTrendValue(String trend) {
		TradingDirection td = TradingDirection.valueOf(trend);
		switch(td) {
			case SHORT : return -1.0;
			case LONG : return 1.0;
			default: return 0.0;
		}
	}
}
