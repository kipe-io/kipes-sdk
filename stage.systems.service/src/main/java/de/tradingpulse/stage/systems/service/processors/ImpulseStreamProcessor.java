package de.tradingpulse.stage.systems.service.processors;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TimeRange;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.stage.systems.recordtypes.ImpulseSourceRecord;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;
import de.tradingpulse.stages.indicators.streams.IndicatorsStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class ImpulseStreamProcessor extends AbstractProcessorFactory {
	
	static ImpulseRecord createImpulseRecordFrom(ImpulseSourceRecord source) {
		DoubleRecord emaRecord = source.getEmaData(); 
		MACDHistogramRecord macdHistogramRecord = source.getMacdHistogramData();
		
		TradingDirection tradingDirection = null;
		
		if(emaRecord == null || macdHistogramRecord == null || emaRecord.getVChange() == null || macdHistogramRecord.getHChange() == null) {
			return null;
		}
		
		if(emaRecord.getVChange() > 0 && macdHistogramRecord.getHChange() > 0) {
			// if both indicators raise then it's a long
			tradingDirection = TradingDirection.LONG;
			
		} else if(emaRecord.getVChange() < 0 && macdHistogramRecord.getHChange() < 0) {
			// if both indicators fall then it's a short
			tradingDirection = TradingDirection.SHORT;
		} else {
			tradingDirection = TradingDirection.NEUTRAL;
		}
		
		return ImpulseRecord.builder()
				.key(source.getKey().deepClone())
				.timeRange(TimeRange.DAY)
				.tradingDirection(tradingDirection)
				.lastTradingDirection(null)
				.build();
	}
	
	@Inject
	private IndicatorsStreamsFacade indicatorsStreamsFacade;
		
	@Inject
	private ConfiguredStreamBuilder builder;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected void initProcessors() throws InterruptedException, ExecutionException {
		// impulse daily
		createImpulseDailyStream(
				SystemsStreamsFacade.TOPIC_IMPULSE_DAILY, 
				indicatorsStreamsFacade.getEma13DailyStream(), 
				indicatorsStreamsFacade.getMacd12269DailyStream());
		
		// impulse weekly daily
		createImpulseDailyStream(
				SystemsStreamsFacade.TOPIC_IMPULSE_WEEKLY, 
				indicatorsStreamsFacade.getEma13WeeklyStream(), 
				indicatorsStreamsFacade.getMacd12269WeeklyStream());
	}
	
	private void createImpulseDailyStream(
			String topic,
			KStream<SymbolTimestampKey, DoubleRecord> emaStream,
			KStream<SymbolTimestampKey, MACDHistogramRecord> macdStream)
	{
		// --------------------------------------------------------------------
		// from
		//   ema_13_weekly
		//
		// join
		//   macd_12_26_9_weekly
		//	 windowSize 0
		//   retentionPeriod 2 years 1 day
		//   as ImpulseSourceRecord
		//
		// transform
		//   change
		//     into ImpulseRecord[timeRange=DAY]
		//
		// transaction
		//   groupBy key.symbol
		//   startsWith true
		//   endsWith false
		//   maxRecords 2
		//   emit ALL
		//   as TransactionRecord<ImpulseRecord>
		//
		// --------------------------------------------------------------------
		
		TopologyBuilder.init(builder)
		.from(
				emaStream, 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(DoubleRecord.class))
		
		.withTopicsBaseName(topic)
		
		// join EMA and MACD into ImpulseSourceRecord by symbol, timestamp
		.<MACDHistogramRecord, ImpulseSourceRecord> join(
				macdStream,
				jsonSerdeRegistry.getSerde(MACDHistogramRecord.class))
			.withWindowSizeAfter(Duration.ZERO)
			.withRetentionPeriod(Duration.ofMillis(this.retentionMs + 86400000L))
			.as(
					(emaData, macdHistogramData)-> 
						ImpulseSourceRecord.builder()
							.key(emaData.getKey())
							.timeRange(emaData.getTimeRange())
							.emaData(emaData)
							.macdHistogramData(macdHistogramData)
							.build(), 
							jsonSerdeRegistry.getSerde(ImpulseSourceRecord.class))
		
		// create ImpulseRecord from ImpulseSourceRecord
		.<SymbolTimestampKey, ImpulseRecord> transform()
			.changeValue(
					(key, value) ->
						ImpulseStreamProcessor.createImpulseRecordFrom(value))
			.asValueType(
					jsonSerdeRegistry.getSerde(ImpulseRecord.class))
		
		// build sequences of size 2 to set the lastTradingDirection
		.<String, ImpulseRecord> sequence()
			.groupBy(
					(key,value) ->
						key.getSymbol(), 
					jsonSerdeRegistry.getSerde(String.class))
			.size(2)
			.as(
					(key, values) -> {
						ImpulseRecord currentRecord = values.get(1);
						currentRecord.setLastTradingDirection(values.get(0).getTradingDirection());
						return currentRecord;
					},
					ImpulseRecord.class,
					jsonSerdeRegistry.getSerde(ImpulseRecord.class))
		
		.to(topic);
	}
}
