package de.tradingpulse.stage.tradingscreens.service.processors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.Produced;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.tradingscreens.recordtypes.ImpulseSignalRecord;
import de.tradingpulse.stage.tradingscreens.recordtypes.SwingSignalType;
import de.tradingpulse.stage.tradingscreens.streams.TradingScreensStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;

@Singleton
public class ImpulseSignalsProcessor extends AbstractProcessorFactory {
	
	@Inject
	private TradingScreensStreamsFacade tradingScreensStreamsFacade;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected void initProcessors() throws Exception {
		createImpulseSignalStream(
				tradingScreensStreamsFacade.getImpulseMomentumSignalsStreamName(),
				SwingSignalType.MOMENTUM);
		
		createImpulseSignalStream(
				tradingScreensStreamsFacade.getImpulsePotentialSignalsStreamName(),
				SwingSignalType.MARKET_TURN_POTENTIAL);
	}
	
	private void createImpulseSignalStream(
			String topicName,
			SwingSignalType swingSignalType)
	{
		tradingScreensStreamsFacade.getImpulseTradingScreenStream()
		.transform(() -> new ImpulseTradingScreenTransformer(swingSignalType))
		.to(topicName, Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(ImpulseSignalRecord.class)));
	}
}
