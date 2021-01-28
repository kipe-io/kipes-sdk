package de.tradingpulse.stage.systems.service.processors;

import java.time.Duration;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;
import de.tradingpulse.stages.indicators.streams.IndicatorsStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class TrendEMAProcessor extends AbstractProcessorFactory {


	static TradingDirection evaluateTrendEMA(DoubleRecord a, DoubleRecord b) {
		if(a.getVChange() == null || b.getVChange() == null) {
			return TradingDirection.NEUTRAL;			
		}
		
		if(a.getVChange() < 0 && b.getVChange() < 0) {
			return TradingDirection.SHORT;
		}
		
		if(a.getVChange() > 0 && b.getVChange() > 0 ) {
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
				SystemsStreamsFacade.TOPIC_TREND_EMA_DAILY, 
				indicatorsStreamsFacade.getEma13DailyStream(), 
				indicatorsStreamsFacade.getEma26DailyStream());

		// long range
		createTopology(
				SystemsStreamsFacade.TOPIC_TREND_EMA_WEEKLY, 
				indicatorsStreamsFacade.getEma13WeeklyStream(), 
				indicatorsStreamsFacade.getEma26WeeklyStream());
	}

	void createTopology(
			String topic,
			KStream<SymbolTimestampKey, DoubleRecord> fastEMAStream,
			KStream<SymbolTimestampKey, DoubleRecord> slowEMAStream)
	{
		// --------------------------------------------------------------------
		// from
		//   fastEMAStream
		//
		// join
		//   slowEMAStream
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
				fastEMAStream, 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(DoubleRecord.class))
		
		.withTopicsBaseName(topic)
		
		.<DoubleRecord, GenericRecord> join(
				slowEMAStream, 
				jsonSerdeRegistry.getSerde(DoubleRecord.class))
		
			.withWindowSize(Duration.ZERO)
			.withRetentionPeriod(Duration.ofMillis(this.retentionMs + 86400000L))
			.as(
					(fast, slow) -> GenericRecord.create()
						.with("key", fast.getKey())
						.with("timeRange", fast.getTimeRange().name())
						.with("timeRangeTimestamp", fast.getTimeRangeTimestamp())
						.with("trendEMA", evaluateTrendEMA(fast, slow).name()),
						jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.to(topic);
			
	}
}
