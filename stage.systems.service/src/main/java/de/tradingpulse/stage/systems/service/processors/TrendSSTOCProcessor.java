package de.tradingpulse.stage.systems.service.processors;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stages.indicators.recordtypes.SSTOCRecord;
import de.tradingpulse.stages.indicators.streams.IndicatorsStreamsFacade;
import io.kipe.streams.kafka.factories.AbstractProcessorFactory;
import io.kipe.streams.kafka.processors.TopologyBuilder;
import io.kipe.streams.recordtypes.GenericRecord;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
class TrendSSTOCProcessor extends AbstractProcessorFactory {

	static TradingDirection evaluateTrendSSTOCSlope(SSTOCRecord sstoc) {
		// TODO current sstoc slope trend indication is way to simplistic
		// - consider local lows and highs to derive slop direction
		// - consider divergences
		if(sstoc.getFChange() == null || sstoc.getSChange() == null) {
			return TradingDirection.NEUTRAL;			
		}
		
		if(sstoc.getFChange() < 0 && sstoc.getSChange() < 0) {
			return TradingDirection.SHORT;
		}
		
		if(sstoc.getFChange() > 0 && sstoc.getSChange() > 0) {
			return TradingDirection.LONG;
		}
		
		return TradingDirection.NEUTRAL;
	}
	
	static TradingDirection evaluateTrendSSTOCValue(SSTOCRecord sstoc) {
		if(sstoc.getFast() == null) {
			return TradingDirection.NEUTRAL;			
		}
		if(sstoc.getFast() > 75) {
			return TradingDirection.SHORT;
		}
		
		if(sstoc.getFast() < 25) {
			return TradingDirection.LONG;
		}
		
		return TradingDirection.NEUTRAL;
	}

	@Inject
	private IndicatorsStreamsFacade indicatorsStreamsFacade;
	
	@Inject
	ConfiguredStreamBuilder streamBuilder;

	@Inject
	JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() throws Exception {
		// short range
		createTopology(
				SystemsStreamsFacade.TOPIC_TREND_SSTOC_DAILY, 
				indicatorsStreamsFacade.getSstoc553DailyStream());

		// long range
		createTopology(
				SystemsStreamsFacade.TOPIC_TREND_SSTOC_WEEKLY, 
				indicatorsStreamsFacade.getSstoc553WeeklyStream());
		
	}

	void createTopology(
			String topic,
			KStream<SymbolTimestampKey, SSTOCRecord> sstocStream)
	{
		// --------------------------------------------------------------------
		// from
		//   sstocStream
		//
		// transform
		//   as
		//     GenericRecord
		//       with ...
		//
		// to
		//   topic
		// --------------------------------------------------------------------
		
		TopologyBuilder.init(streamBuilder)
		
		.from(
				sstocStream, 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(SSTOCRecord.class))
		
		.withTopicsBaseName(topic)
		
		.<SymbolTimestampKey, GenericRecord> transform()
			.changeValue(
					(key, sstoc) -> 
						GenericRecord.create()
						.with("key", sstoc.getKey())
						.with("timeRange", sstoc.getTimeRange().name())
						.with("timeRangeTimestamp", sstoc.getTimeRangeTimestamp())
						.with("trendSSTOCSlope", evaluateTrendSSTOCSlope(sstoc).name())
						.with("trendSSTOCValue", evaluateTrendSSTOCValue(sstoc).name()))
			.asValueType(
					jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.to(topic);
			
	}
}
