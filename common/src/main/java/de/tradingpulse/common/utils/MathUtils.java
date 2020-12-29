package de.tradingpulse.common.utils;

public class MathUtils {

	public static double round(double value, int digits) {
		double f = Math.pow(10, digits);
		return Math.round(value * f) / f; 
	}
	
	private MathUtils() {}
}
