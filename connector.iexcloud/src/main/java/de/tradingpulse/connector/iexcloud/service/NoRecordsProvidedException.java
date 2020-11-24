package de.tradingpulse.connector.iexcloud.service;

public class NoRecordsProvidedException extends Exception {

	private static final long serialVersionUID = 1L;

	private final String symbol;
	
	public NoRecordsProvidedException(String symbol, IEXCloudRange range) {
		super(String.format("%s: no records received for range %s and all smaller ranges. Consider to remove the symbol.", symbol, range));
		this.symbol = symbol;
	}
	
	public String getSymbol() {
		return this.symbol;
	}
}
