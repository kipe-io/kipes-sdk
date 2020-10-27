package de.tradingpulse.common.stream.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ImpulseData {

	private SymbolTimestampKey key;
	private TradingDirection direction;
	private TradingDirection lastDirection;
}
