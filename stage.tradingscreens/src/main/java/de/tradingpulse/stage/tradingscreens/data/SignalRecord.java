package de.tradingpulse.stage.tradingscreens.data;

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
public class SignalRecord extends AbstractIncrementalAggregateRecord {

	private EntrySignal entry;
	private ExitSignal exit;
}
