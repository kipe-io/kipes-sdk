package de.tradingpulse.stage.indicators.service.processors;

import de.tradingpulse.stages.indicators.aggregates.SSTOCAggregate;
import de.tradingpulse.stages.indicators.recordtypes.SSTOCRecord;

public class SSTOCTransformer extends AbstractOHLCVRecordTransformer<SSTOCAggregate, SSTOCRecord>{

	private final int n;
	private final int p1;
	private final int p2;
	
	public SSTOCTransformer(String storeName, int n, int p1, int p2) {
		super(storeName);
		
		this.n = n;
		this.p1 = p1;
		this.p2 = p2;
	}
	
	@Override
	protected SSTOCAggregate getInitialAggregate() {
		return new SSTOCAggregate(n, p1, p2);
	}
}
