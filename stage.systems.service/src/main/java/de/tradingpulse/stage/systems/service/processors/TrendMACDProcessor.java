package de.tradingpulse.stage.systems.service.processors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;
import de.tradingpulse.stages.indicators.streams.IndicatorsStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class TrendMACDProcessor extends AbstractProcessorFactory {
	
	static TradingDirection evaluateTrendMACDHistogram(MACDHistogramRecord macd) {
		if(macd.getHChange() == null) {
			return TradingDirection.NEUTRAL;			
		}
		
		if(macd.getHChange() < 0) {
			return TradingDirection.SHORT;
		}
		
		if(macd.getHChange() > 0) {
			return TradingDirection.LONG;
		}
		
		return TradingDirection.NEUTRAL;
	}
	
	static TradingDirection evaluateTrendMACDLinesSlope(MACDHistogramRecord macd) {
		if(macd.getMChange() == null || macd.getSChange() == null) {
			return TradingDirection.NEUTRAL;
		}
		
		if(macd.getMChange() < 0 && macd.getSChange() < 0) {
			return TradingDirection.SHORT;
		}
		
		if(macd.getMChange() > 0 && macd.getSChange() > 0) {
			return TradingDirection.LONG;
		}
		
		return TradingDirection.NEUTRAL;
	}
	
	static TradingDirection evaluateTrendMACDValue(MACDHistogramRecord macd) {
		if(macd.getHistogram() == null || macd.getHChange() == null || macd.getMacd() == null) {
			return TradingDirection.NEUTRAL;
		}
		
		if(macd.getHistogram() > 0 && macd.getHChange() < 0 && macd.getMacd() > 0 ) {
			return TradingDirection.SHORT;
		}
		
		if(macd.getHistogram() < 0 && macd.getHChange() > 0 && macd.getMacd() < 0 ) {
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
				SystemsStreamsFacade.TOPIC_TREND_MACD_DAILY, 
				indicatorsStreamsFacade.getMacd12269DailyStream());

		// long range
		createTopology(
				SystemsStreamsFacade.TOPIC_TREND_MACD_WEEKLY, 
				indicatorsStreamsFacade.getMacd12269WeeklyStream());
	}

	void createTopology(
			String topic,
			KStream<SymbolTimestampKey, MACDHistogramRecord> macdStream)
	{
		// --------------------------------------------------------------------
		// from
		//   macdStream
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
				macdStream, 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(MACDHistogramRecord.class))
		
		.withTopicsBaseName(topic)
		
		.<SymbolTimestampKey, GenericRecord> transform()
			.changeValue(
					(key, macd) -> 
						GenericRecord.create()
						.with("key", macd.getKey())
						.with("timeRange", macd.getTimeRange().name())
						.with("timeRangeTimestamp", macd.getTimeRangeTimestamp())
						.with("trendMacdHistogram", evaluateTrendMACDHistogram(macd).name())
						.with("trendMACDLinesSlope", evaluateTrendMACDLinesSlope(macd).name())
						.with("trendMACDValue", evaluateTrendMACDValue(macd).name()))
			.asValueType(
					jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.to(topic);
			
	}

}
