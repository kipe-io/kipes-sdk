package de.tradingpulse.common.stream.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ImpulseSourceData {
	
	private SymbolTimestampKey key;
	private DoubleData emaData;
	private MACDHistogramData macdHistogramData;
}