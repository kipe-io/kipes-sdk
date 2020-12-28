package de.tradingpulse.connector.iexcloud.service;

public abstract class IEXCloudException extends Exception {

	private static final long serialVersionUID = 2L;

	protected IEXCloudException(String message) {
		super(message);
	}
}
