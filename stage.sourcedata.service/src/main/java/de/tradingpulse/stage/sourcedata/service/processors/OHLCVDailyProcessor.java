package de.tradingpulse.stage.sourcedata.service.processors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Produced;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import de.tradingpulse.stage.sourcedata.streams.SourceDataStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;

@Singleton
class OHLCVDailyProcessor extends AbstractProcessorFactory {

	@Inject
	private SourceDataStreamsFacade sourceDataStreamsFacade;

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;
	
	@Override
	protected void initProcessors() {
		sourceDataStreamsFacade.getOhlcvDailyRawStream()
		// map OHLCVDataRaw -> OHLCVData
		.map((key, rawRecord) -> {
			OHLCVRecord record = OHLCVRecord.from(rawRecord);
			return new KeyValue<>(record.getKey(), record);
			})
		// push to sink
		.to(sourceDataStreamsFacade.getOhlcvDailyStreamName(), Produced.with(
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(OHLCVRecord.class)));
	}
}
