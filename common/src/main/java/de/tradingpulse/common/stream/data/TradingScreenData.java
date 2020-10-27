package de.tradingpulse.common.stream.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradingScreenData {

	private SymbolTimestampKey key;
	private TradingDirection lastDirection;
	private TradingDirection direction;
}
