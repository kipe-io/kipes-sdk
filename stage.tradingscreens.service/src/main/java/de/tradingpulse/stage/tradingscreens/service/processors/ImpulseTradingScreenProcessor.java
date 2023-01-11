package de.tradingpulse.stage.tradingscreens.service.processors;

import java.time.Duration;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import io.kipe.streams.kafka.factories.AbstractProcessorFactory;
import io.kipe.streams.kafka.processors.TopologyBuilder;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
class ImpulseTradingScreenProcessor extends AbstractProcessorFactory {
		
	@Inject
	private SystemsStreamsFacade systemsStreamsFacade;
	
	@Inject
	private ConfiguredStreamBuilder streamBuilder;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() throws Exception {
		// --------------------------------------------------------------------
		// from
		//   impulse_daily
		// inner join
		//	 impulse_weekly
		//   on impulse_daily.key = impulse_weekly.key	# works since impulse_weekly.key is in daily increments
		//   window size 0 retention 2 years 1 day
		//   as ImpulseTradingScreenRecord
		// dedup
		//   groupBy key.symbol
		//	 advanceBy longRangeImpulseRecord.impulseDescriptor and shortRangeImpulseRecord.impulseDescriptor
		//   emitFirst
		// to
		//	 impulse_trading_screen
		// --------------------------------------------------------------------
		
		TopologyBuilder
		.init(streamBuilder)
		
		.withTopicsBaseName(TradingScreensStreamsFacade.TOPIC_IMPULSE_TRADING_SCREEN)
		
		.from(
				systemsStreamsFacade.getImpulseDailyStream(), 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(ImpulseRecord.class))
		
		.<ImpulseRecord, ImpulseTradingScreenRecord> join(
				systemsStreamsFacade.getImpulseWeeklyStream(),
				jsonSerdeRegistry.getSerde(ImpulseRecord.class))
		
			.withWindowSize(Duration.ZERO)
			.withRetentionPeriod(Duration.ofMillis(this.retentionMs + 86400000L))
			.as(
					(shortTimeRangeImpulseRecord, longTimeRangeImpulseRecord) -> 
						ImpulseTradingScreenRecord.builder()
						.key(shortTimeRangeImpulseRecord.getKey())
						.timeRange(shortTimeRangeImpulseRecord.getTimeRange())
						.shortRangeImpulseRecord(shortTimeRangeImpulseRecord)
						.longRangeImpulseRecord(longTimeRangeImpulseRecord)
						.build(), 
						jsonSerdeRegistry.getSerde(ImpulseTradingScreenRecord.class))
			
		.<String,String> dedup()
			.groupBy(
				(key, record) ->
					key.getSymbol(),
				jsonSerdeRegistry.getSerde(String.class))
			.advanceBy(
				(key, record) ->
					record.getLongRangeImpulseRecord().getImpulseDescriptor() + ":" + record.getShortRangeImpulseRecord().getImpulseDescriptor())
			.emitFirst()
			
		.to(
				TradingScreensStreamsFacade.TOPIC_IMPULSE_TRADING_SCREEN);
	}
}
