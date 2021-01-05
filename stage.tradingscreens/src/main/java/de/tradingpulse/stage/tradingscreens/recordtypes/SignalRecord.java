package de.tradingpulse.stage.tradingscreens.recordtypes;

import de.tradingpulse.common.stream.aggregates.DeepCloneable;
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
public class SignalRecord extends AbstractIncrementalAggregateRecord 
implements DeepCloneable<SignalRecord>
{

	private SignalType signalType;
	private String strategyKey;
	
	@Override
	public SignalRecord deepClone() {
		SignalRecord clone = new SignalRecord(this.signalType, this.strategyKey);
		clone.cloneValuesFrom(this);
		return clone;
	}
}
