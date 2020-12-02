package de.tradingpulse.stages.indicators.recordtypes;

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class SSTOCRecord extends AbstractIncrementalAggregateRecord {

	private Double fast;
	private Double fChange;
	private Double slow;
	private Double sChange;

}
