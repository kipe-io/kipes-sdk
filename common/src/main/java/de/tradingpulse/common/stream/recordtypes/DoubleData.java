package de.tradingpulse.common.stream.recordtypes;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DoubleData {

	private SymbolTimestampKey key;
	private Double value;
	private Double vChange;
}
