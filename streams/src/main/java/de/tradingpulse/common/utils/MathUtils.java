package de.tradingpulse.common.utils;

public class MathUtils {

	public static double round(double value, int digits) {
		double f = Math.pow(10, digits);
		return Math.round(value * f) / f; 
	}
	
	public static int getPrecision(double value) {
		double absValue = Math.abs(value);
		double positiveFractionalPart = absValue - Math.floor(absValue);
		
		if(positiveFractionalPart == 0.0) {
			return 0;
		}
		
		int precision = 1;
		while(precision < 17) { // max precision of double
			double poweredValue = absValue * Math.pow(10, precision);
			double poweredFractionalPart = poweredValue - Math.floor(poweredValue);
			if(poweredFractionalPart < 0.0000000000000001) {
				break;
			}
			
			precision += 1;
		}
		
		return precision;
	}
	
	private MathUtils() {}
}
