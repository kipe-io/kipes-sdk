package de.tradingpulse.stage.systems.service.processors;

import java.time.Duration;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.recordtypes.Trends;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import de.tradingpulse.streams.recordtypes.GenericRecord;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class TrendAggregatorProcessor extends AbstractProcessorFactory {
	
	@Inject
	private SystemsStreamsFacade systemsStreamsFacade;
	
	@Inject
	ConfiguredStreamBuilder streamBuilder;

	@Inject
	JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() throws Exception {
		// short range
		createTopology(
				SystemsStreamsFacade.TOPIC_TRENDS_DAILY, 
				systemsStreamsFacade.getTrendEMADailyStream(),
				systemsStreamsFacade.getTrendMACDDailyStream(),
				systemsStreamsFacade.getTrendSSTOCDailyStream());

		// long range
		createTopology(
				SystemsStreamsFacade.TOPIC_TRENDS_WEEKLY, 
				systemsStreamsFacade.getTrendEMAWeeklyStream(),
				systemsStreamsFacade.getTrendMACDWeeklyStream(),
				systemsStreamsFacade.getTrendSSTOCWeeklyStream());
	}
	
	void createTopology(
			String topic,
			KStream<SymbolTimestampKey, GenericRecord> trendEMAStream,
			KStream<SymbolTimestampKey, GenericRecord> trendMACDStream,
			KStream<SymbolTimestampKey, GenericRecord> trendSSTOCStream)
	{
		// --------------------------------------------------------------------
		// from
		//   trendEMAStream
		//
		// join
		//   trendMACDStream
		//   windowSize 0
		//   retentionPeriod 2 years 1 day
		//   as
		//     GenericRecord
		//       with ...
		//
		// join
		//   trendSSTOCStream
		//   windowSize 0
		//   retentionPeriod 2 years 1 day
		//   as
		//     GenericRecord
		//       with ...
		//
		// to
		//   topic
		// --------------------------------------------------------------------
		
		TopologyBuilder.init(streamBuilder)
		
		.from(
				trendEMAStream, 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.withTopicsBaseName(topic+"_ema_macd")
		
		.<GenericRecord, GenericRecord> join(
				trendMACDStream, 
				jsonSerdeRegistry.getSerde(GenericRecord.class))
		
			.withWindowSize(Duration.ZERO)
			.withRetentionPeriod(Duration.ofMillis(this.retentionMs + 86400000L))
			.as(
					(trendEMA, trendMACD) -> GenericRecord.create()
						.withValueFrom("key", trendEMA)
						.withValueFrom("timeRange", trendEMA)
						.withValueFrom("timeRangeTimestamp", trendEMA)
						.with("trends", Trends.builder()
								
								.ema(trendEMA.getString("trendEMA"))
								
								.macdHistogram(trendMACD.getString("trendMACDHistogram"))
								.macdLinesSlope(trendMACD.getString("trendMACDLinesSlope"))
								.macdValue(trendMACD.getString("trendMACDValue"))
								
								.build()),
						jsonSerdeRegistry.getSerde(GenericRecord.class))
			
		.withTopicsBaseName(topic+"_ema_macd_sstoc")
		
		.<GenericRecord, GenericRecord> join(
				trendSSTOCStream, 
				jsonSerdeRegistry.getSerde(GenericRecord.class))
		
			.withWindowSize(Duration.ZERO)
			.withRetentionPeriod(Duration.ofMillis(this.retentionMs + 86400000L))
			.as(
					(trendEMAandMACD, trendSSTOC) -> {
						GenericRecord r = trendEMAandMACD.copy();
						Trends trends = r.get("trends");
						
						trends.setSstocSlope(trendSSTOC.getString("trendSSTOCSlope"));
						trends.setSstocValue(trendSSTOC.getString("trendSSTOCValue"));
						
						return r;
					},
						jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.to(topic);
		
	}

}
