package de.tradingpulse.common.stream.recordtypes;

import de.tradingpulse.common.stream.aggregates.DeepCloneable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SymbolTimestampKey implements DeepCloneable<SymbolTimestampKey>{
	
	private String symbol;
	private long timestamp;
	
	@Override
	public SymbolTimestampKey deepClone() {
		return new SymbolTimestampKey(this.symbol, this.timestamp);
	}
}
