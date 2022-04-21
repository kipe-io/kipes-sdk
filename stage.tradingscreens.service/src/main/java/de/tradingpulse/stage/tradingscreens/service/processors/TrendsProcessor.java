package de.tradingpulse.stage.tradingscreens.service.processors;

import java.time.Duration;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import de.tradingpulse.streams.recordtypes.GenericRecord;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class TrendsProcessor extends AbstractProcessorFactory {
	
	@Inject
	SystemsStreamsFacade systemsStreamFacade;
	
	@Inject
	ConfiguredStreamBuilder streamBuilder;

	@Inject
	JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() throws Exception {
		createTopology(
				TradingScreensStreamsFacade.TOPIC_TRENDS, 
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
		// join
		//   longRangeTrendsStream
		//   windowSize 0
		//   retentionPeriod 2 years 1 day
		//   as
		//     GenericRecord
		//       with 
		//
		// to
		//   topic
		// --------------------------------------------------------------------
		
		TopologyBuilder
		.init(streamBuilder)
		
		.from(
				shortRangeTrendsStream, 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.withTopicsBaseName(topic)
		
		.<GenericRecord, GenericRecord> join(
				longRangeTrendsStream,
				jsonSerdeRegistry.getSerde(GenericRecord.class))
				
			.withWindowSize(Duration.ZERO)
			.withRetentionPeriod(Duration.ofMillis(this.retentionMs + 86400000L))
			.as(
					(shortRange, longRange) -> GenericRecord.create()
						.withValueFrom("key", shortRange)
						.withValueFrom("timeRange", shortRange)
						.withValueFrom("timeRangeTimestamp", shortRange)
						
						.with("shortRangeTrends", shortRange.get("trends"))
						.with("longRangeTrends", longRange.get("trends")),
						
						jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.to(topic);
	}
}
