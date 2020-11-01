package de.tradingpulse.common.stream.recordtypes;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SymbolTimestampKey {
	
	private String symbol;
	private long timestamp;
}
