package de.tradingpulse.stage.tradingscreens.service.processors;

import java.time.Duration;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SwingSignalType;
import de.tradingpulse.stage.tradingscreens.service.strategies.CompositeStrategy;
import de.tradingpulse.stage.tradingscreens.service.strategies.SwingMarketTurnPotentialEntryStrategy;
import de.tradingpulse.stage.tradingscreens.service.strategies.SwingMarketTurnPotentialExitStrategy;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import de.tradingpulse.streams.recordtypes.GenericRecord;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class SignalsProcessor extends AbstractProcessorFactory {
	
	@Inject
	SystemsStreamsFacade systemsStreamsFacade;
	
	@Inject
	TradingScreensStreamsFacade tradingScreensStreamsFacade;

	@Inject
	ConfiguredStreamBuilder streamsBuilder;
	
	@Inject
	JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() {
		createTopology(
				tradingScreensStreamsFacade.getImpulseTradingScreenStream(),
				tradingScreensStreamsFacade.getTrendsStream());
	}
	
	void createTopology(
			KStream<SymbolTimestampKey, ImpulseTradingScreenRecord> impulseTradingScreenStream,
			KStream<SymbolTimestampKey, GenericRecord> trendsStream )
	{
		// --------------------------------------------------------------------
		// from
		//   impulse_trading_screen
		//
		// transform
		//   into
		//     ImpulseTradingScreenToSignalFunction:SignalRecord[]
		//   as
		//     SignalRecord
		//
		// dedup
		//   groupBy key.symbol, strategyKey
		//   advanceBy signalType
		//   emitFirst
		//
		// join
		//   trends_value
		//   on {keys}
		//   window size 0 retention 2 years 1 day
		//   as SignalRecord
		//      signalRecord.shortRangeTrendsValue = ...
		//
		// to
		//   signals_daily
		// --------------------------------------------------------------------
		
		TopologyBuilder.init(streamsBuilder)
		
		.from(
				impulseTradingScreenStream, 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(ImpulseTradingScreenRecord.class))
		
		.<SymbolTimestampKey,SignalRecord> transform()
			.newValues(
					new CompositeStrategy(
							SwingSignalType.SWING_MARKET_TURN_POTENTIAL.name(), 
							new SwingMarketTurnPotentialEntryStrategy(), 
							new SwingMarketTurnPotentialExitStrategy()))
			
			.asValueType(
					jsonSerdeRegistry.getSerde(SignalRecord.class))
		
		.withTopicsBaseName(TradingScreensStreamsFacade.TOPIC_SIGNAL_DAILY)
		
		.<String[], String> dedup()
			.groupBy(
					(key, value) ->
						 new String[]{
								 key.getSymbol(),
								 value.getStrategyKey()},
					jsonSerdeRegistry.getSerde(String[].class))
			.advanceBy(
					(key, value) ->
						value.getSignalType().name())
			.emitFirst()
		
		.<GenericRecord, SignalRecord> join(
				trendsStream,
				jsonSerdeRegistry.getSerde(GenericRecord.class))
			.withWindowSize(Duration.ZERO)
			.withRetentionPeriod(Duration.ofMillis(this.retentionMs + 86400000L ))
			.as(
					(signal, trends) -> signal.deepClone()
								.withTrendsFrom(trends),
					jsonSerdeRegistry.getSerde(SignalRecord.class))
		.to(
				TradingScreensStreamsFacade.TOPIC_SIGNAL_DAILY);
	}
	
	// ------------------------------------------------------------------------
	// 
	// ------------------------------------------------------------------------

	
}
