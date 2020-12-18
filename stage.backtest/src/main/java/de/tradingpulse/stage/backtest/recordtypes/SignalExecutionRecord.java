package de.tradingpulse.stage.backtest.recordtypes;

import de.tradingpulse.common.stream.recordtypes.AbstractIncrementalAggregateRecord;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Record describing the earliest execution of a Signal. <br>
 * <br>
 * The execution of a Signal always happens-after the Signal. The key of this
 * record equals the key of the referenced {@link #ohlcvRecord}. 
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class SignalExecutionRecord extends AbstractIncrementalAggregateRecord {

	public static SignalExecutionRecord from(SignalRecord signalRecord, OHLCVRecord ohlcvRecord) {
		return SignalExecutionRecord.builder()
				.key(ohlcvRecord.getKey().deepClone())
				.timeRange(ohlcvRecord.getTimeRange())
				.signalRecord(signalRecord)
				.ohlcvRecord(ohlcvRecord)
				.build();
	}
	
	private SignalRecord signalRecord;
	private OHLCVRecord ohlcvRecord;
}
