package de.tradingpulse.common.stream.recordtypes;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MACDHistogramData {

	private SymbolTimestampKey key;
	
	private Double macd;
	private Double mChange;
	
	private Double signal;
	private Double sChange;
	
	private Double histogram;
	private Double hChange;
}
