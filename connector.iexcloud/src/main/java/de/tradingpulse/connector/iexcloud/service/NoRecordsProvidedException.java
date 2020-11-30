package de.tradingpulse.connector.iexcloud.service;

public class NoRecordsProvidedException extends IEXCloudException {

	private static final long serialVersionUID = 2L;

	private final String symbol;
	
	public NoRecordsProvidedException(String symbol, IEXCloudRange range) {
		super(String.format("%s: no records received for range %s and all smaller ranges. Consider to remove the symbol.", symbol, range));
		this.symbol = symbol;
	}
	
	public NoRecordsProvidedException(String symbol) {
		super(String.format("%s: no records received. Consider to remove the symbol.", symbol));
		this.symbol = symbol;
	}
	
	public String getSymbol() {
		return this.symbol;
	}
}
