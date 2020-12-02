package de.tradingpulse.stages.indicators.aggregates;

import de.tradingpulse.common.stream.aggregates.DeepCloneable;
import de.tradingpulse.stage.sourcedata.recordtypes.OHLCVRecord;

public interface OHLCVRecordAggregate <T, A> extends DeepCloneable<A> {

	T aggregate(OHLCVRecord record);
}
