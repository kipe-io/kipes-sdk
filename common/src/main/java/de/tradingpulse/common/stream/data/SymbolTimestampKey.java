package de.tradingpulse.common.stream.data;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SymbolTimestampKey {

	public static final SymbolTimestampKey from(OHLCVDataRaw rawData) {
		final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		
		return builder()
				.symbol(rawData.getSymbol())
				.timestamp(
						LocalDate.parse(rawData.getDate(), dtf)
						.atStartOfDay()
						.toEpochSecond(ZoneOffset.UTC)
						* 1000)
				.build();
	}
	
	private String symbol;
	private long timestamp;
}
