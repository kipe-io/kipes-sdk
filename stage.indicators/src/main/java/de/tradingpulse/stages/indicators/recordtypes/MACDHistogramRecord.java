package de.tradingpulse.stages.indicators.recordtypes;

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class MACDHistogramRecord extends AbstractIncrementalAggregateRecord {

	private Double macd;
	private Double mChange;
	
	private Double signal;
	private Double sChange;
	
	private Double histogram;
	private Double hChange;
}
