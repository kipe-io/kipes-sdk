package de.tradingpulse.stage.tradingscreens.service.processors;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.ENTRY_LONG;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.ENTRY_SHORT;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.EXIT_LONG;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.EXIT_SHORT;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.common.stream.recordtypes.TradingDirection;
import de.tradingpulse.stage.systems.recordtypes.ImpulseRecord;
import de.tradingpulse.stage.systems.streams.SystemsStreamsFacade;
import de.tradingpulse.stage.tradingscreens.recordtypes.EntrySignal;
import de.tradingpulse.stage.tradingscreens.recordtypes.ExitSignal;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.Type;
import de.tradingpulse.stage.tradingscreens.recordtypes.SwingSignalType;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

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
					new MomentumStrategy())
			
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

	static class MomentumStrategy
	implements BiFunction<SymbolTimestampKey, ImpulseTradingScreenRecord, Iterable<SignalRecord>> {

		@Override
		public Iterable<SignalRecord> apply(SymbolTimestampKey key, ImpulseTradingScreenRecord value) {
			// Momentum rules:
			// 
			// - longRange.current dictates trading direction
			// - longRange.current == NEUTRAL means no ENTRY
			//
			// ENTRY(longRange.current) when
			//          longRange.current != NEUTRAL
			//      AND shortRange.change = longRange.current
			//      AND shortRange.current = longRange.current
			//
			// EXIT(longRange.current) when
			//          longRange.current != NEUTRAL
			//		AND longRange.change == NEUTRAL
			//      AND shortRange.change != NEUTRAL
			//      AND shortRange.last == longRange.last
			//		AND shortRange.change != longRange.current
			//
			// EXIT(longRange.last) when
			//          longRange.last != NEUTRAL
			//      AND longRange.change != NEUTRAL
			//      AND shortRange.last == longRange.last
			
			List<SignalRecord> signals = new ArrayList<>(2);
			signals.add(evalExitSignal(value));
			signals.add(evalEntrySignal(value));
			signals.removeIf(Objects::isNull);
			return signals;
		}

		private SignalRecord evalExitSignal(ImpulseTradingScreenRecord value) {
			ImpulseRecord longRange = value.getLongRangeImpulseRecord();
			ImpulseRecord shortRange = value.getShortRangeImpulseRecord();
			
			if(	longRange.getTradingDirection() != TradingDirection.NEUTRAL
				&& longRange.getChangeTradingDirection() == TradingDirection.NEUTRAL
				&& shortRange.getChangeTradingDirection() != TradingDirection.NEUTRAL
				&& shortRange.getLastTradingDirection() == longRange.getLastTradingDirection()
				&& shortRange.getChangeTradingDirection() != longRange.getTradingDirection())
			{
				return SignalRecord.builder()
						.key(value.getKey())
						.timeRange(value.getTimeRange())
						.strategyKey(SwingSignalType.SWING_MOMENTUM.name())
						.signalType(SignalType.from(Type.EXIT, longRange.getTradingDirection()))
						.build();
			}
			
			if(	longRange.getLastTradingDirection() != TradingDirection.NEUTRAL
				&& longRange.getChangeTradingDirection() != TradingDirection.NEUTRAL
				&& shortRange.getLastTradingDirection() == longRange.getLastTradingDirection())
			{
				return SignalRecord.builder()
						.key(value.getKey())
						.timeRange(value.getTimeRange())
						.strategyKey(SwingSignalType.SWING_MOMENTUM.name())
						.signalType(SignalType.from(Type.EXIT, longRange.getLastTradingDirection()))
						.build();
			}
			
			return null;
		}

		private SignalRecord evalEntrySignal(ImpulseTradingScreenRecord value) {
			ImpulseRecord longRange = value.getLongRangeImpulseRecord();
			ImpulseRecord shortRange = value.getShortRangeImpulseRecord();
			
			if(	longRange.getTradingDirection() != TradingDirection.NEUTRAL
				&& shortRange.getChangeTradingDirection() == longRange.getTradingDirection()
				&& shortRange.getTradingDirection() == longRange.getTradingDirection())
			{
				return SignalRecord.builder()
						.key(value.getKey())
						.timeRange(value.getTimeRange())
						.strategyKey(SwingSignalType.SWING_MOMENTUM.name())
						.signalType(SignalType.from(Type.ENTRY, longRange.getTradingDirection()))
						.build();
			}
			
			return null;
		}
		
	}
	
	// ------------------------------------------------------------------------
	// ImpulseSignalRecord -> SignalRecord
	// ------------------------------------------------------------------------

	public static class ImpulseTradingScreenToSignalFunction
	implements BiFunction<SymbolTimestampKey, ImpulseTradingScreenRecord, Iterable<SignalRecord>> {

		private static final Logger LOG = LoggerFactory.getLogger(ImpulseTradingScreenToSignalFunction.class);
		
		@Override
		public Iterable<SignalRecord> apply(SymbolTimestampKey key, ImpulseTradingScreenRecord value) {
			
			List<SignalRecord> signals = createExitSignals(value);
			signals.addAll(createEntrySignals(value));
			
			return signals;
		}
		
		private List<SignalRecord> createEntrySignals(ImpulseTradingScreenRecord value) {
			List<SignalRecord> entrySignals = new LinkedList<>();
			
			Optional<EntrySignal> entry = value.getEntrySignal();
			if(entry.isEmpty()) {
				return entrySignals;
			}
			
			EntrySignal entrySignal = entry.get();
			SignalType signalType = entrySignal.is(SHORT)? ENTRY_SHORT : ENTRY_LONG;
			
			// if there's the strange case that we got both an entry and exit signal
			// for the same TradingDirection we ignore the entry
			Optional<ExitSignal> exit = value.getExitSignal();
			if(exit.isPresent() && exit.get().is(signalType.getTradingDirection())) {
				LOG.warn("Found both exit and entry signal for the same trading direction. Check your strategy! Ignoring entry signal. {}", value);
				return entrySignals;
			}
			
			if(entrySignal.is(SwingSignalType.SWING_MOMENTUM)) {
				entrySignals.add(
						createSignalRecord(
								value, 
								signalType, 
								SwingSignalType.SWING_MOMENTUM.name()));
			}
			
			if(entrySignal.is(SwingSignalType.SWING_MARKET_TURN_POTENTIAL)) {
				entrySignals.add(
						createSignalRecord(
								value, 
								signalType, 
								SwingSignalType.SWING_MARKET_TURN_POTENTIAL.name()));
			}
			
			return entrySignals;
		}
		
		private List<SignalRecord> createExitSignals(ImpulseTradingScreenRecord value) {
			List<SignalRecord> exitSignals = new LinkedList<>();
			
			Optional<ExitSignal> exit = value.getExitSignal();
			
			if(exit.isEmpty()) {
				return exitSignals;
			}
			
			ExitSignal exitSignal = exit.get();
			SignalType signalType = exitSignal.is(SHORT)? EXIT_SHORT : EXIT_LONG;
			
			if(exitSignal.is(SwingSignalType.SWING_MOMENTUM)) {
				exitSignals.add(
						createSignalRecord(
								value, 
								signalType, 
								SwingSignalType.SWING_MOMENTUM.name()));
			}
			
			if(exitSignal.is(SwingSignalType.SWING_MARKET_TURN_POTENTIAL)) {
				exitSignals.add(
						createSignalRecord(
								value, 
								signalType, 
								SwingSignalType.SWING_MARKET_TURN_POTENTIAL.name()));
			}
			
			return exitSignals;
		}
		
		private SignalRecord createSignalRecord(
				ImpulseTradingScreenRecord value, 
				SignalType signalType,
				String strategyKey)
		{
			return SignalRecord.builder()
					.key(value.getKey().deepClone())
					.timeRange(value.getTimeRange())
					.signalType(signalType)
					.strategyKey(strategyKey)
					.build();
		}
	}
}
