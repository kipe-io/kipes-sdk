package de.tradingpulse.stage.systems.recordtypes;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stages.indicators.recordtypes.DoubleRecord;
import de.tradingpulse.stages.indicators.recordtypes.MACDHistogramRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ImpulseSourceData {
	
	private SymbolTimestampKey key;
	private DoubleRecord emaData;
	private MACDHistogramRecord macdHistogramData;
}