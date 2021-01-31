package de.tradingpulse.stage.systems.recordtypes;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
@ToString
public class Trends {
	
	// ------------------------------------------------------------------------
	// ! when changing the fields you need to update the related ksql as well !
	// ------------------------------------------------------------------------
	
	private String ema;
	
	private String macdHistogram;
	private String macdLinesSlope;
	private String macdValue;
	
	private String sstocSlope;
	private String sstocValue;
}