package de.tradingpulse.connector.iexcloud.service;

import retrofit2.Response;

public class FetchException extends IEXCloudException {

	private static final long serialVersionUID = 1L;

	public FetchException(String symbol, IEXCloudRange range, Response<?> response) {
		super(String.format(
				"%s: couldn't fetch records for range %s, response code was: %s:\"%s\"", 
				symbol, 
				range, 
				response.code(), 
				response.message()));
	}
}
