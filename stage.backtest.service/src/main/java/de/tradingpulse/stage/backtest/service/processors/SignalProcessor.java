package de.tradingpulse.stage.backtest.service.processors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.recordtypes.SignalRecord;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseSignalRecord;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;

@Singleton
public class SignalProcessor extends AbstractProcessorFactory {

	@Inject
	private TradingScreensStreamsFacade tradingScreensStreamsFacade;

	@Inject
	private BacktestStreamsFacade backtestStreamsFacade;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() {
		createSignalStream(tradingScreensStreamsFacade.getImpulseMomentumSignalsStream());
		createSignalStream(tradingScreensStreamsFacade.getImpulsePotentialSignalsStream());
	}

	private void createSignalStream(KStream<SymbolTimestampKey, ImpulseSignalRecord> impulseSignalSourceStream) {
		impulseSignalSourceStream
		.flatMap(new SignalRecordMapper())
		.to(backtestStreamsFacade.getSignalDailyStreamName(), Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(SignalRecord.class)));
	}
}
