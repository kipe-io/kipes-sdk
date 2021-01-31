package de.tradingpulse.stage.tradingscreens.recordtypes;

import de.tradingpulse.common.stream.aggregates.DeepCloneable;
import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import de.tradingpulse.common.stream.recordtypes.GenericRecord;
import de.tradingpulse.stage.systems.recordtypes.Trends;
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
public class SignalRecord extends AbstractIncrementalAggregateRecord 
implements DeepCloneable<SignalRecord>
{
	// ------------------------------------------------------------------------
	// ! when changing the fields you need to update the related ksql as well !
	// ------------------------------------------------------------------------
	
	private SignalType signalType;
	private String strategyKey;
	private Trends shortRangeTrends;
	private Trends longRangeTrends;
	
	@Override
	public SignalRecord deepClone() {
		SignalRecord clone = new SignalRecord(this.signalType, this.strategyKey, this.shortRangeTrends, this.longRangeTrends);
		clone.cloneValuesFrom(this);
		return clone;
	}
	
	public SignalRecord withTrendsFrom(GenericRecord trendsRecord) {
		this.shortRangeTrends = trendsRecord.get("shortRangeTrends");
		this.longRangeTrends = trendsRecord.get("longRangeTrends");
		
		return this;
	}
}
