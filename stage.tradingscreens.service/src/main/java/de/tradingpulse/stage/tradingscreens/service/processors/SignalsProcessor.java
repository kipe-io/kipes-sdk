package de.tradingpulse.stage.tradingscreens.service.processors;

import static de.tradingpulse.common.stream.recordtypes.TradingDirection.SHORT;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.ENTRY_LONG;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.ENTRY_SHORT;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.EXIT_LONG;
import static de.tradingpulse.stage.tradingscreens.recordtypes.SignalType.EXIT_SHORT;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.recordtypes.EntrySignal;
import de.tradingpulse.stage.tradingscreens.recordtypes.ExitSignal;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseTradingScreenRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SignalType;
import de.tradingpulse.stage.tradingscreens.recordtypes.SwingSignalType;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
public class SignalsProcessor extends AbstractProcessorFactory {
	
	@Inject
	private TradingScreensStreamsFacade tradingScreensStreamsFacade;

	@Inject
	private ConfiguredStreamBuilder streamsBuilder;
	
	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() {
		// --------------------------------------------------------------------
		// from
		//   impulse_trading_screen
		// transform
		//   into
		//     asSignalRecords(value):SignalRecord[]
		//   as
		//     SignalRecord
		// to
		//   signals_daily
		// --------------------------------------------------------------------
		
		TopologyBuilder.init(streamsBuilder)
		
		.from(
				tradingScreensStreamsFacade.getImpulseTradingScreenStream(), 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(ImpulseTradingScreenRecord.class))
		
		.<SignalRecord> transform()
			.intoMultiRecords(
					new ImpulseTradingScreenToSignalFunction())
			
			.as(
					jsonSerdeRegistry.getSerde(SignalRecord.class))
		
		.to(
				tradingScreensStreamsFacade.getSignalDailyStreamName());
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
